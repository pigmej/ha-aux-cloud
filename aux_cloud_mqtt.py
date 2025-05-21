#!/usr/bin/env python3
"""MQTT interface for AUX Cloud API to enable integration with Homey."""

import asyncio
import base64
import json
import os
import logging
import signal
import sys
import threading
import time
from typing import Dict, List, Any, Optional
import paho.mqtt.client as mqtt
import concurrent.futures
import yaml

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_components.aux_cloud.api.aux_cloud import (
    AuxCloudAPI,
    AuxApiError,
    ExpiredTokenError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
_LOGGER = logging.getLogger(__name__)

# MQTT topics
TOPIC_PREFIX = "aux_cloud"
DEVICE_DISCOVERY_TOPIC = f"{TOPIC_PREFIX}/devices"
SET_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/set"
STATE_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/state"
PARAM_STATE_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/{{param}}"
PARAM_SET_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/{{param}}/set"
AVAILABLE_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/available"
APPLY_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/apply"
COMMAND_RESPONSE_TOPIC = f"{TOPIC_PREFIX}/response"
ERROR_TOPIC = f"{TOPIC_PREFIX}/error"


class AuxCloudMQTTBridge:
    """Bridge between AUX Cloud API and MQTT.

    This class provides a bridge between the AUX Cloud API and MQTT,
    enabling control of AUX devices (air conditioners, heat pumps, etc.)
    via MQTT. This allows integration with Homey and other smart home platforms
    that support MQTT.

    Features:
    - Automatic device discovery
    - Real-time state updates via polling and WebSocket
    - Command handling via MQTT
    - Status reporting and error handling
    """

    # Store command queue for thread-safe operations
    _command_queue = []

    def __init__(
        self,
        mqtt_host: str,
        mqtt_port: int,
        mqtt_username: Optional[str] = None,
        mqtt_password: Optional[str] = None,
        mqtt_client_id: str = "aux_cloud_mqtt",
        aux_email: str = None,
        aux_password: str = None,
        aux_region: str = "eu",
        update_interval: int = 60,
        enable_websocket: bool = True,
    ):
        """Initialize the MQTT bridge."""
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.mqtt_client_id = mqtt_client_id
        self.aux_email = aux_email
        self.aux_password = aux_password
        self.aux_region = aux_region.lower()  # Ensure lowercase for API
        self.update_interval = update_interval
        self.enable_websocket = enable_websocket

        self._mqtt_client = None
        self.aux_api = None
        self.devices = {}
        self.families = {}
        self.connected = False
        self.running = False
        self.device_tasks = {}

    async def async_setup(self):
        """Set up the bridge."""
        _LOGGER.info("Setting up AUX Cloud MQTT Bridge")

        # Clear command queue at start
        AuxCloudMQTTBridge._command_queue = []

        # Store the event loop for async operations from threads
        self._event_loop = asyncio.get_event_loop()

        # Initialize the API with just the region
        self.aux_api = AuxCloudAPI(self.aux_region)

        try:
            # Set the email and password before calling login
            self.aux_api.email = self.aux_email
            self.aux_api.password = self.aux_password

            # Now login to the API
            await self.aux_api.login()

            if not self.aux_api.is_logged_in():
                _LOGGER.error("Failed to login to AUX Cloud API")
                return False

            _LOGGER.info("Successfully logged in to AUX Cloud API")

            # Set up MQTT client
            self._setup_mqtt()

            # Get initial devices and families
            await self._update_devices_and_families()

            # Set up WebSocket if enabled AFTER getting devices
            if (
                self.enable_websocket
                and self.aux_api.loginsession
                and self.aux_api.userid
            ):
                await self._setup_websocket()
                await (
                    self._subscribe_to_device_updates()
                )  # Moved here after device loading

            # Start device update loops
            self.running = True
            for device_id in self.devices:
                self.device_tasks[device_id] = asyncio.create_task(
                    self._device_update_loop(device_id)
                )

            return True
        except AuxApiError as err:
            _LOGGER.error("Error setting up AUX Cloud API: %s", err)
            return False

    async def _setup_websocket(self):
        """Set up WebSocket connection."""
        if hasattr(self.aux_api, "initialize_websocket"):
            try:
                await self.aux_api.initialize_websocket()
                _LOGGER.info("WebSocket connection initialized")

                # Add listener for device state updates
                if hasattr(self.aux_api, "ws_api") and self.aux_api.ws_api:
                    self.aux_api.ws_api.add_websocket_listener(self._handle_ws_message)
                    _LOGGER.info("WebSocket listener added")

                    # Monitor WebSocket connection and reconnect if needed
                    self.device_tasks["websocket_monitor"] = asyncio.create_task(
                        self._websocket_monitor()
                    )
            except Exception as err:
                _LOGGER.error("Failed to initialize WebSocket: %s", err)
                # Try to reconnect after a delay
                self.device_tasks["websocket_reconnect"] = asyncio.create_task(
                    self._websocket_reconnect_after_delay()
                )
        else:
            _LOGGER.warning("WebSocket functionality not available in API")

    async def _websocket_monitor(self):
        """Monitor WebSocket connection and reconnect if needed."""
        while self.running:
            try:
                if not hasattr(self.aux_api, "ws_api") or not self.aux_api.ws_api:
                    _LOGGER.warning("WebSocket API not initialized, attempting setup")
                    await self._setup_websocket()

                ws_api = self.aux_api.ws_api
                if ws_api.websocket is None or ws_api.websocket.closed:
                    _LOGGER.warning(
                        "WebSocket connection lost, forcing reinitialization"
                    )
                    await ws_api.close_websocket()  # Cleanup old connection
                    await self._setup_websocket()

                    # Only resubscribe if we have devices
                    if self.devices:
                        await self._subscribe_to_device_updates()
                    else:
                        _LOGGER.warning(
                            "No devices available for WebSocket resubscription"
                        )

                await asyncio.sleep(15)  # Check more frequently
            except asyncio.CancelledError:
                _LOGGER.info("WebSocket monitor task cancelled")
                break
            except Exception as err:
                _LOGGER.error("Error in WebSocket monitor: %s", err)
                await asyncio.sleep(30)  # Wait longer after an error

    async def _websocket_reconnect_after_delay(self, delay=10):
        """Try to reconnect WebSocket after a delay."""
        await asyncio.sleep(delay)
        try:
            await self.aux_api.initialize_websocket()
            if hasattr(self.aux_api, "ws_api") and self.aux_api.ws_api:
                self.aux_api.ws_api.add_websocket_listener(self._handle_ws_message)
                await self._subscribe_to_device_updates()
            _LOGGER.info("WebSocket reconnected successfully after delay")
        except Exception as err:
            _LOGGER.error("Failed to reconnect WebSocket after delay: %s", err)

    async def _subscribe_to_device_updates(self):
        """Subscribe to WebSocket updates for all devices."""
        try:
            if not hasattr(self.aux_api, "ws_api") or not self.aux_api.ws_api:
                _LOGGER.warning("WebSocket API not available for subscription")
                return

            if not self.devices:
                _LOGGER.warning("No devices available to subscribe to")
                return

            timestamp = time.time()
            devices = []

            # Build device list from our known devices
            for device_id, device in self.devices.items():
                devices.append(
                    {
                        "devSession": device.get("devSession", ""),
                        "endpointId": device.get("endpointId", ""),
                        "gatewayId": "",  # Empty for direct connections
                        # "pid": device.get("pid", ""),
                        "pid": "000000000000000000000000c0620000",
                    }
                )

            # Send subscription message
            await self.aux_api.ws_api.send_data(
                {
                    "data": {"devList": devices},
                    "messageid": timestamp,
                    "msgtype": "subreset",  # subreset gets current state + future updates
                    "topic": "devpush",
                }
            )
            _LOGGER.info("Subscribed to WebSocket updates for %d devices", len(devices))
        except Exception as err:
            _LOGGER.error("Error subscribing to device updates: %s", err)

    async def _handle_ws_message(self, message):
        """Handle WebSocket message."""
        _LOGGER.debug("Received WebSocket message: %s", message)

        # Process message and update device state if applicable
        try:
            if message.get("msgtype") == "push" and message.get("topic") == "devpush":
                data = message.get("data", {}).get("data")
                # payload = data.get("payload", {}).get("data")

                # if not payload:
                #     _LOGGER.warning("WebSocket message missing payload data")
                #     return

                # Try to decode base64 payload if present
                try:
                    decoded = json.loads(base64.b64decode(data).decode())
                    _LOGGER.debug("Decoded WebSocket data: %s", decoded)
                except Exception as decode_err:
                    _LOGGER.error("Error decoding WebSocket data: %s", decode_err)
                    return

                device_id = decoded.get("did")
                if not device_id:
                    _LOGGER.warning("WebSocket message missing device ID")
                    return

                if device_id not in self.devices:
                    _LOGGER.warning(
                        "WebSocket message for unknown device ID: %s", device_id
                    )
                    return

                # Extract state data from the decoded payload
                state_update = {}
                for key, value in decoded.items():
                    if key not in ["did", "pid"]:  # Skip device identifiers
                        state_update[key] = value

                if state_update:
                    _LOGGER.info(
                        "Using state data from WebSocket message for device: %s",
                        device_id,
                    )
                    self._publish_device_state(device_id, state_update)
                    _LOGGER.info(
                        "Updated device state from WebSocket notification: %s",
                        device_id,
                    )
            elif (
                message.get("msgtype") == "devnotify"
            ):  # TODO: not sure about that part at all, got that message once and kinda no idea why.
                data = message.get("data", {})
                device_id = data.get("did")

                if not device_id:
                    _LOGGER.warning("WebSocket message missing device ID: %s", message)
                    return

                if device_id not in self.devices:
                    _LOGGER.warning(
                        "WebSocket message for unknown device ID: %s", device_id
                    )
                    return

                # Extract state data directly from the message if possible
                params = data.get("params")
                if params:
                    _LOGGER.info(
                        "Using state data from WebSocket message for device: %s",
                        device_id,
                    )
                    self._publish_device_state(device_id, params)
                    _LOGGER.info(
                        "Updated device state from WebSocket notification: %s",
                        device_id,
                    )
                else:
                    # Fallback to fetching current state
                    device = self.devices[device_id]
                    _LOGGER.info(
                        "Scheduling state fetch for device after WebSocket notification: %s",
                        device_id,
                    )
                    # Schedule a state refresh
                    self._schedule_state_fetch(device_id, device)
        except Exception as err:
            _LOGGER.error("Error handling WebSocket message: %s", err)

    def _setup_mqtt(self):
        """Set up the MQTT client.

        This method initializes the MQTT client with the current protocol version.
        """

    def _setup_mqtt(self):
        """Set up the MQTT client."""
        # Initialize MQTT client
        self._mqtt_client = mqtt.Client(
            client_id=self.mqtt_client_id, protocol=mqtt.MQTTv311
        )
        _LOGGER.info("MQTT client initialized")

        if self.mqtt_username and self.mqtt_password:
            self._mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)

        self._mqtt_client.on_connect = self._on_mqtt_connect
        self._mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self._mqtt_client.on_message = self._on_mqtt_message

        try:
            _LOGGER.info(
                "Connecting to MQTT broker at %s:%s", self.mqtt_host, self.mqtt_port
            )
            self._mqtt_client.connect(self.mqtt_host, self.mqtt_port)
            self._mqtt_client.loop_start()
        except Exception as err:
            _LOGGER.error("Failed to connect to MQTT broker: %s", err)
            raise

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection."""
        if rc == 0:
            self.connected = True
            _LOGGER.info("Connected to MQTT broker")

            # Subscribe to all set commands
            self._mqtt_client.subscribe(f"{TOPIC_PREFIX}/+/set")

            # Subscribe to parameter-specific set commands
            self._mqtt_client.subscribe(f"{TOPIC_PREFIX}/+/+/set")

            # Subscribe to apply commands
            self._mqtt_client.subscribe(f"{TOPIC_PREFIX}/+/apply")

            # Just publish discovery right away from this thread
            # No need to schedule through the event loop for this simple operation
            self._publish_device_discovery()
        else:
            _LOGGER.error("Failed to connect to MQTT broker with code %s", rc)

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection."""
        self.connected = False
        _LOGGER.warning("Disconnected from MQTT broker with code %s", rc)

    def _on_mqtt_message(self, client, userdata, msg):
        """Handle incoming MQTT messages."""
        topic = msg.topic

        try:
            payload = msg.payload.decode("utf-8")
            _LOGGER.debug("Received MQTT message on topic %s: %s", topic, payload)

            parts = topic.split("/")

            # Handle main device set topic (aux_cloud/{device_id}/set)
            if len(parts) == 3 and parts[2] == "set":
                device_id = parts[1]

                try:
                    command_data = json.loads(payload)
                except json.JSONDecodeError:
                    _LOGGER.error("Invalid JSON payload: %s", payload)
                    self._publish_error(
                        f"Invalid JSON payload for device {device_id}: {payload}"
                    )
                    return

                # Store command for processing in main loop
                with threading.Lock():
                    AuxCloudMQTTBridge._command_queue.append(
                        (
                            device_id,
                            command_data.copy(),
                            False,
                        )  # False = no forced refresh
                    )
                _LOGGER.debug(f"Command queued for device {device_id}: {command_data}")

            # Handle apply command (aux_cloud/{device_id}/apply)
            elif len(parts) == 3 and parts[2] == "apply":
                device_id = parts[1]

                if payload.lower() in ["true", "1", "yes", "on"]:
                    _LOGGER.info(f"Received apply command for device {device_id}")

                    # Execute apply command immediately instead of queuing
                    if device_id in self.devices:
                        # Schedule immediate execution in an async-safe way
                        asyncio.run_coroutine_threadsafe(
                            self._handle_apply_command(device_id), self._event_loop
                        )
                        _LOGGER.debug(
                            f"Apply command executed immediately for device {device_id}"
                        )
                    else:
                        _LOGGER.warning(
                            f"Device {device_id} not found for apply command"
                        )

            # Handle parameter-specific set topic (aux_cloud/{device_id}/{param}/set)
            elif len(parts) == 4 and parts[3] == "set":
                device_id = parts[1]
                param = parts[2]

                try:
                    # For parameter-specific topics, payload is the direct value
                    # Convert it to the appropriate type if possible
                    value = payload

                    # Try to convert to number or boolean if applicable
                    if payload.lower() == "true":
                        value = True
                    elif payload.lower() == "false":
                        value = False
                    else:
                        try:
                            if "." in payload:
                                value = float(payload)
                            else:
                                value = int(payload)
                        except ValueError:
                            # Keep as string if not convertible
                            pass

                    # Create command data with single parameter
                    command_data = {param: value}

                    # Store command for processing in main loop
                    with threading.Lock():
                        AuxCloudMQTTBridge._command_queue.append(
                            (
                                device_id,
                                command_data.copy(),
                                False,
                            )  # False = no forced refresh
                        )
                    _LOGGER.debug(
                        f"Parameter command queued for device {device_id}: {command_data}"
                    )
                except Exception as err:
                    _LOGGER.error("Error processing parameter value: %s", err)
                    self._publish_error(
                        f"Error processing value for device {device_id}, parameter {param}: {err}"
                    )
            else:
                _LOGGER.error("Invalid topic format: %s", topic)
        except UnicodeDecodeError:
            _LOGGER.error("Failed to decode payload as UTF-8 on topic: %s", topic)
        except Exception as err:
            _LOGGER.error("Error handling MQTT message: %s", err)

    async def _handle_apply_command(self, device_id: str):
        """Handle apply command immediately - forces a device state refresh."""
        if device_id not in self.devices:
            self._publish_error(f"Unknown device ID: {device_id}")
            return

        _LOGGER.info(f"Handling immediate apply command for device {device_id}")

        try:
            # Check if we're logged in, otherwise re-login
            if not self.aux_api.is_logged_in():
                await self.aux_api.login()

            # Get current device state
            device = self.devices[device_id]
            updated_state = await self.aux_api.get_device_params(device)

            # Publish updated state
            self._publish_device_state(device_id, updated_state)

            # Also run any queued commands for this device
            commands_to_process = []
            with threading.Lock():
                # Find and remove commands for this device from the queue
                remaining_commands = []
                for cmd in AuxCloudMQTTBridge._command_queue:
                    if len(cmd) >= 2 and cmd[0] == device_id:
                        commands_to_process.append(cmd)
                    else:
                        remaining_commands.append(cmd)
                AuxCloudMQTTBridge._command_queue = remaining_commands

            # Process any commands found for this device
            for cmd in commands_to_process:
                try:
                    if len(cmd) == 2:  # Handle old format commands
                        await self._handle_device_command(cmd[0], cmd[1], True)
                    else:
                        await self._handle_device_command(cmd[0], cmd[1], True)
                except Exception as cmd_err:
                    _LOGGER.error(
                        f"Error processing queued command during apply: {cmd_err}"
                    )

            # Publish success response
            self._mqtt_client.publish(
                COMMAND_RESPONSE_TOPIC,
                json.dumps(
                    {
                        "device_id": device_id,
                        "success": True,
                        "message": "Device state refreshed immediately",
                    }
                ),
            )
        except ExpiredTokenError:
            _LOGGER.warning("Token expired during apply, attempting to re-login")
            try:
                await self.aux_api.login()
                await self._handle_apply_command(device_id)
            except Exception as err:
                self._publish_error(f"Failed to refresh device {device_id}: {err}")
        except Exception as err:
            _LOGGER.error(f"Error refreshing device {device_id}: {err}")
            self._publish_error(f"Error refreshing device {device_id}: {err}")

    async def _handle_device_command(
        self, device_id: str, command_data: Dict[str, Any], force_refresh: bool = False
    ):
        """Handle device command from MQTT."""
        if device_id not in self.devices:
            self._publish_error(f"Unknown device ID: {device_id}")
            return

        _LOGGER.info(
            "Handling command for device %s: %s (force_refresh: %s)",
            device_id,
            command_data,
            force_refresh,
        )

        try:
            # Check if we're logged in, otherwise re-login
            if not self.aux_api.is_logged_in():
                await self.aux_api.login()

            # Send command to device if we have any parameters to set
            device = self.devices[device_id]
            if command_data:
                await self.aux_api.set_device_params(device, command_data)

            # Get updated state after command
            updated_state = await self.aux_api.get_device_params(device)

            # Publish updated state
            self._publish_device_state(device_id, updated_state)

            # Publish command response
            response_data = {
                "device_id": device_id,
                "success": True,
                "message": "Command executed successfully",
            }

            if command_data:
                response_data["command"] = command_data
            elif force_refresh:
                response_data["message"] = "Device state refreshed"

            self._mqtt_client.publish(
                COMMAND_RESPONSE_TOPIC,
                json.dumps(response_data),
            )
        except ExpiredTokenError:
            _LOGGER.warning("Token expired, attempting to re-login")
            try:
                await self.aux_api.login()
                # Retry command after re-login
                await self._handle_device_command(device_id, command_data)
            except Exception as err:
                self._publish_error(f"Failed to re-login: {err}")
        except Exception as err:
            _LOGGER.error("Error executing command for device %s: %s", device_id, err)
            self._publish_error(
                f"Error executing command for device {device_id}: {err}"
            )

    async def _update_devices_and_families(self):
        """Update devices and families from the API."""
        try:
            # Get families
            families = await self.aux_api.get_families()
            self.families = {family["familyid"]: family for family in families}

            # Get devices for each family
            all_devices = {}
            for family_id in self.families:
                devices = await self.aux_api.get_devices(family_id)
                for device in devices:
                    print(device)
                    all_devices[device["endpointId"]] = device

            self.devices = all_devices
            _LOGGER.info(
                "Updated %s devices from %s families",
                len(self.devices),
                len(self.families),
            )

            # Publish device discovery after update
            self._publish_device_discovery()

            return True
        except ExpiredTokenError:
            _LOGGER.warning("Token expired, attempting to re-login")
            await self.aux_api.login()
            return await self._update_devices_and_families()
        except Exception as err:
            _LOGGER.error("Error updating devices and families: %s", err)
            return False

    def _publish_device_discovery(self):
        """Publish device discovery information to MQTT."""
        if not self.connected:
            return

        # Create a simplified device list for discovery
        device_list = []
        for device_id, device in self.devices.items():
            # Create parameter topics based on common parameters
            param_topics = {}

            # Define common parameters for all devices
            common_params = [
                "power",
                "mode",
                "temperature",
                "fan_speed",
                "current_temperature",
            ]
            for param in common_params:
                param_topics[param] = {
                    "state": PARAM_STATE_TOPIC_TEMPLATE.format(
                        device_id=device_id, param=param
                    ),
                    "set": PARAM_SET_TOPIC_TEMPLATE.format(
                        device_id=device_id, param=param
                    ),
                }

            device_info = {
                "id": device_id,
                "name": device.get("friendlyName", f"AUX Device {device_id}"),
                "type": device.get("type", "unknown"),
                "online": device.get("online", False),
                "family_id": device.get("familyId"),
                "topics": {
                    "set": SET_TOPIC_TEMPLATE.format(device_id=device_id),
                    "state": STATE_TOPIC_TEMPLATE.format(device_id=device_id),
                    "available": AVAILABLE_TOPIC_TEMPLATE.format(device_id=device_id),
                    "apply": APPLY_TOPIC_TEMPLATE.format(device_id=device_id),
                    "parameters": param_topics,
                },
            }
            device_list.append(device_info)

        self._mqtt_client.publish(
            DEVICE_DISCOVERY_TOPIC, json.dumps({"devices": device_list}), retain=True
        )

    def _publish_device_state(self, device_id: str, state: Dict[str, Any]):
        """Publish device state to MQTT."""
        if not self.connected:
            return

        # Publish full state to main state topic
        topic = STATE_TOPIC_TEMPLATE.format(device_id=device_id)
        self._mqtt_client.publish(topic, json.dumps(state), retain=True)

        # Publish availability
        available_topic = AVAILABLE_TOPIC_TEMPLATE.format(device_id=device_id)
        self._mqtt_client.publish(available_topic, "online", retain=True)

        # Publish individual parameters to separate topics
        for param, value in state.items():
            param_topic = PARAM_STATE_TOPIC_TEMPLATE.format(
                device_id=device_id, param=param
            )

            # Format the value appropriately
            if isinstance(value, (dict, list)):
                formatted_value = json.dumps(value)
            else:
                formatted_value = str(value)

            self._mqtt_client.publish(param_topic, formatted_value, retain=True)

    def _publish_error(self, error_message: str):
        """Publish error message to MQTT."""
        if not self.connected:
            return

        self._mqtt_client.publish(
            ERROR_TOPIC,
            json.dumps({"timestamp": time.time(), "message": error_message}),
        )

    async def _device_update_loop(self, device_id: str):
        """Update loop for a single device."""
        while self.running:
            try:
                # Process any queued commands
                commands_to_process = []
                with threading.Lock():
                    if AuxCloudMQTTBridge._command_queue:
                        commands_to_process = AuxCloudMQTTBridge._command_queue.copy()
                        AuxCloudMQTTBridge._command_queue.clear()

                force_refresh_this_cycle = False
                for cmd in commands_to_process:
                    try:
                        if (
                            len(cmd) == 2
                        ):  # Handle old format commands for backward compatibility
                            device_id, command_data = cmd
                            force_refresh = False
                        else:
                            device_id, command_data, force_refresh = cmd

                        _LOGGER.info(
                            f"Processing queued command for {device_id}: {command_data} (force_refresh: {force_refresh})"
                        )
                        await self._handle_device_command(
                            device_id, command_data, force_refresh
                        )
                        if force_refresh:
                            force_refresh_this_cycle = True
                    except Exception as cmd_err:
                        _LOGGER.error(f"Error processing queued command: {cmd_err}")

                # If we had a force refresh command, don't wait for the next interval
                if force_refresh_this_cycle:
                    continue

                if not self.aux_api.is_logged_in():
                    await self.aux_api.login(self.aux_email, self.aux_password)

                # Get device state
                device = self.devices[device_id]
                device_state = await self.aux_api.get_device_params(device)

                # Publish device state
                self._publish_device_state(device_id, device_state)

                # Wait for the next update, unless we just did a refresh
                await asyncio.sleep(self.update_interval)
            except ExpiredTokenError:
                _LOGGER.warning("Token expired, attempting to re-login")
                try:
                    await self.aux_api.login(self.aux_email, self.aux_password)
                except Exception as err:
                    _LOGGER.error("Failed to re-login: %s", err)
                    await asyncio.sleep(60)  # Wait a bit longer after login failure
            except Exception as err:
                _LOGGER.error("Error updating device %s: %s", device_id, err)
                await asyncio.sleep(60)  # Wait a bit longer after error

    async def async_stop(self):
        """Stop the bridge."""
        _LOGGER.info("Stopping AUX Cloud MQTT Bridge")
        self.running = False

        # Cancel device update tasks
        for task in self.device_tasks.values():
            try:
                task.cancel()
                # Wait for task to be cancelled, but with timeout
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except asyncio.TimeoutError:
                    _LOGGER.warning(f"Task cancellation timed out")
            except Exception as err:
                _LOGGER.warning(f"Error cancelling task: {err}")

        # Set device availability status to offline
        if self.connected:
            for device_id in self.devices:
                try:
                    offline_topic = AVAILABLE_TOPIC_TEMPLATE.format(device_id=device_id)
                    self._mqtt_client.publish(offline_topic, "offline", retain=True)
                except Exception as err:
                    _LOGGER.warning(f"Error publishing offline status: {err}")

        # Close WebSocket connection if open
        if hasattr(self.aux_api, "ws_api") and self.aux_api.ws_api:
            try:
                await self.aux_api.ws_api.close_websocket()
            except Exception as err:
                _LOGGER.warning(f"Error closing WebSocket: {err}")

        # Stop MQTT client
        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception as err:
                _LOGGER.warning(f"Error disconnecting MQTT client: {err}")

        # Clear the command queue
        with threading.Lock():
            AuxCloudMQTTBridge._command_queue.clear()

    def _schedule_state_fetch(self, device_id, device):
        """Mark device for state refresh on next update cycle."""
        _LOGGER.info(f"Marking device {device_id} for immediate state refresh")
        # Execute apply command immediately instead of queuing
        asyncio.run_coroutine_threadsafe(
            self._handle_apply_command(device_id), self._event_loop
        )


async def load_config(config_path: str):
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as err:
        _LOGGER.error("Error loading config file: %s", err)
        raise


async def main():
    """Run the AUX Cloud MQTT Bridge.

    Performs initial setup and validation:
    1. Checks for compatible Python version (3.7+)
    2. Loads configuration
    3. Initializes and starts the bridge

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    # Check Python version
    if sys.version_info < (3, 7):
        _LOGGER.error("This script requires Python 3.7 or newer")
        return 1

    # Handle graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        _LOGGER.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    # Load config
    config_path = os.environ.get("CONFIG_PATH", "aux_cloud_mqtt_config.yaml")
    try:
        config = await load_config(config_path)
    except Exception:
        _LOGGER.error("Failed to load configuration. Exiting.")
        return 1

    # Set log level
    log_level = config.get("settings", {}).get("log_level", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level))

    # Create and start bridge
    bridge = AuxCloudMQTTBridge(
        mqtt_host=config.get("mqtt", {}).get("host", "localhost"),
        mqtt_port=int(config.get("mqtt", {}).get("port", 1883)),
        mqtt_username=config.get("mqtt", {}).get("username"),
        mqtt_password=config.get("mqtt", {}).get("password"),
        mqtt_client_id=config.get("mqtt", {}).get("client_id", "aux_cloud_mqtt"),
        aux_email=config.get("aux_cloud", {}).get("email"),
        aux_password=config.get("aux_cloud", {}).get("password"),
        aux_region=config.get("aux_cloud", {}).get("region", "eu"),
        update_interval=int(config.get("settings", {}).get("update_interval", 60)),
        enable_websocket=bool(config.get("settings", {}).get("enable_websocket", True)),
    )

    setup_success = await bridge.async_setup()
    if not setup_success:
        _LOGGER.error("Failed to set up AUX Cloud MQTT Bridge. Exiting.")
        return 1

    # Wait for stop signal
    await stop_event.wait()

    # Stop bridge
    await bridge.async_stop()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
