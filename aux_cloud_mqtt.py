#!/usr/bin/env python3
"""MQTT interface for AUX Cloud API to enable integration with Homey."""

import asyncio
import base64
import json
import os
import logging
import signal
import sys
import time
from typing import Dict, Any, Optional
import paho.mqtt.client as mqtt
import yaml

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_components.aux_cloud.api.aux_cloud import (
    AuxCloudAPI,
    AuxApiError,
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
    """Bridge between AUX Cloud API and MQTT for smart home integration."""

    def __init__(
        self,
        mqtt_host: str,
        mqtt_port: int,
        mqtt_username: Optional[str] = None,
        mqtt_password: Optional[str] = None,
        mqtt_client_id: str = "aux_cloud_mqtt",
        aux_email: Optional[str] = None,
        aux_password: Optional[str] = None,
        aux_region: str = "eu",
        update_interval: int = 60,
        device_refresh_interval: int = 3600,
        enable_websocket: bool = True,
        apply_timeout: int = 30,
    ):
        """Initialize the MQTT bridge."""
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.mqtt_client_id = mqtt_client_id
        self.aux_email = aux_email
        self.aux_password = aux_password
        self.aux_region = aux_region.lower()
        self.update_interval = update_interval
        self.device_refresh_interval = device_refresh_interval
        self.enable_websocket = enable_websocket
        self.apply_timeout = apply_timeout

        self._mqtt_client: Optional[mqtt.Client] = None
        self.aux_api: Optional[AuxCloudAPI] = None
        self.devices = {}
        self.families = {}
        self.connected = False
        self.running = False
        self.tasks = {}
        self.command_queue = asyncio.Queue()
        self._loop = None

        # Bulk apply functionality
        self.pending_commands = {}  # device_id -> {param: value}
        self.apply_timers = {}  # device_id -> timer_task

    async def async_setup(self):
        """Set up the bridge."""
        _LOGGER.info("Setting up AUX Cloud MQTT Bridge")

        try:
            # Store event loop for thread-safe operations
            self._loop = asyncio.get_running_loop()

            # Initialize API
            await self._setup_api()

            # Set up MQTT
            self._setup_mqtt()

            # Load initial data
            await self._update_devices_and_families()

            # Set up WebSocket if enabled
            if self.enable_websocket:
                await self._setup_websocket()

            # Start background tasks
            self._start_background_tasks()

            self.running = True
            return True

        except Exception as err:
            _LOGGER.error("Error setting up bridge: %s", err)
            return False

    async def _setup_api(self):
        """Initialize and login to AUX Cloud API."""
        if not self.aux_email or not self.aux_password:
            raise AuxApiError("AUX email and password are required")

        self.aux_api = AuxCloudAPI(self.aux_region)

        await self.aux_api.login(self.aux_email, self.aux_password)
        if not self.aux_api.is_logged_in():
            raise AuxApiError("Failed to login to AUX Cloud API")

        _LOGGER.info("Successfully logged in to AUX Cloud API")

    def _setup_mqtt(self):
        """Set up the MQTT client."""
        self._mqtt_client = mqtt.Client(
            client_id=self.mqtt_client_id, protocol=mqtt.MQTTv311
        )

        if self.mqtt_username and self.mqtt_password:
            self._mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)

        self._mqtt_client.on_connect = self._on_mqtt_connect
        self._mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self._mqtt_client.on_message = self._on_mqtt_message

        _LOGGER.info(
            "Connecting to MQTT broker at %s:%s", self.mqtt_host, self.mqtt_port
        )
        self._mqtt_client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        self._mqtt_client.loop_start()

    async def _setup_websocket(self):
        """Set up WebSocket connection."""
        if not self.aux_api or not hasattr(self.aux_api, "initialize_websocket"):
            _LOGGER.warning("WebSocket functionality not available in API")
            return

        try:
            await self.aux_api.initialize_websocket()
            if hasattr(self.aux_api, "ws_api") and self.aux_api.ws_api:
                self.aux_api.ws_api.add_websocket_listener(self._handle_ws_message)
                await self._subscribe_to_device_updates()
                _LOGGER.info("WebSocket connection established")
        except Exception as err:
            _LOGGER.error("Failed to initialize WebSocket: %s", err)

    def _start_background_tasks(self):
        """Start all background tasks."""
        self.tasks["command_processor"] = asyncio.create_task(self._command_processor())
        self.tasks["device_updater"] = asyncio.create_task(self._device_update_loop())
        self.tasks["device_refresher"] = asyncio.create_task(
            self._device_refresh_loop()
        )

    async def _subscribe_to_device_updates(self):
        """Subscribe to WebSocket updates for all devices."""
        if (
            not self.aux_api
            or not hasattr(self.aux_api, "ws_api")
            or not self.aux_api.ws_api
            or not self.devices
        ):
            return

        try:
            devices = [
                {
                    "devSession": device.get("devSession", ""),
                    "endpointId": device.get("endpointId", ""),
                    "gatewayId": "",
                    "pid": "000000000000000000000000c0620000",
                }
                for device in self.devices.values()
            ]

            await self.aux_api.ws_api.send_data(
                {
                    "data": {"devList": devices},
                    "messageid": time.time(),
                    "msgtype": "subreset",
                    "topic": "devpush",
                }
            )
            _LOGGER.info("Subscribed to WebSocket updates for %d devices", len(devices))
        except Exception as err:
            _LOGGER.error("Error subscribing to device updates: %s", err)

    async def _handle_ws_message(self, message):
        """Handle WebSocket message."""
        try:
            if message.get("msgtype") == "push" and message.get("topic") == "devpush":
                data = message.get("data", {}).get("data")
                if not data:
                    return

                decoded = json.loads(base64.b64decode(data).decode())
                device_id = decoded.get("did")

                if device_id and device_id in self.devices:
                    state_update = {
                        k: v for k, v in decoded.items() if k not in ["did", "pid"]
                    }
                    if state_update:
                        self._publish_device_state(device_id, state_update)
                        _LOGGER.debug(
                            "Updated device state from WebSocket: %s", device_id
                        )
        except Exception as err:
            _LOGGER.error("Error handling WebSocket message: %s", err)

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """Handle MQTT connection."""
        if rc == 0:
            self.connected = True
            _LOGGER.info("Connected to MQTT broker")
            _LOGGER.debug("MQTT host: %s", self.mqtt_host)

            # Subscribe to command topics
            if self._mqtt_client:
                self._mqtt_client.subscribe(f"{TOPIC_PREFIX}/+/set")
                self._mqtt_client.subscribe(f"{TOPIC_PREFIX}/+/+/set")
                self._mqtt_client.subscribe(f"{TOPIC_PREFIX}/+/apply")

            self._publish_device_discovery()
        else:
            _LOGGER.error("Failed to connect to MQTT broker with code %s", rc)

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """Handle MQTT disconnection."""
        self.connected = False
        if rc != 0:
            _LOGGER.warning(
                "Disconnected from MQTT broker with code %s", rc
            )
        else:
            _LOGGER.info("Disconnected from MQTT broker normally")

    def _on_mqtt_message(self, client, userdata, msg):
        """Handle incoming MQTT messages."""
        try:
            payload = msg.payload.decode("utf-8")
            topic_parts = msg.topic.split("/")

            _LOGGER.debug("Received MQTT message on topic %s: %s", msg.topic, payload)

            if len(topic_parts) == 3 and topic_parts[2] == "set":
                # Device command
                device_id = topic_parts[1]
                try:
                    command_data = json.loads(payload)
                    self._queue_command("device_command", device_id, command_data)
                except json.JSONDecodeError as err:
                    _LOGGER.error("Invalid JSON in MQTT message: %s", err)

            elif len(topic_parts) == 3 and topic_parts[2] == "apply":
                # Apply command
                device_id = topic_parts[1]
                if payload.lower() in ["true", "1", "yes", "on"]:
                    self._queue_command("apply_command", device_id, {})

            elif len(topic_parts) == 4 and topic_parts[3] == "set":
                # Parameter-specific command
                device_id = topic_parts[1]
                param = topic_parts[2]
                value = self._parse_value(payload)
                command_data = {param: value}
                self._queue_command("device_command", device_id, command_data)
            else:
                _LOGGER.debug("Ignored message on topic: %s", msg.topic)

        except UnicodeDecodeError as err:
            _LOGGER.error("Failed to decode MQTT message payload: %s", err)
        except Exception as err:
            _LOGGER.error("Error handling MQTT message: %s", err)

    def _parse_value(self, payload: str):
        """Parse string payload to appropriate type."""
        if payload.lower() == "true":
            return True
        elif payload.lower() == "false":
            return False

        try:
            return float(payload) if "." in payload else int(payload)
        except ValueError:
            return payload

    def _queue_command(self, command_type: str, device_id: str, data: Dict[str, Any]):
        """Thread-safely queue a command for processing."""
        try:
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self.command_queue.put((command_type, device_id, data)), self._loop
                )
        except Exception as err:
            _LOGGER.error("Error queuing command: %s", err)

    async def _schedule_auto_apply(self, device_id: str):
        """Schedule auto-apply for a device after timeout."""
        # Cancel existing timer for this device
        if device_id in self.apply_timers:
            self.apply_timers[device_id].cancel()

        # Schedule new auto-apply
        async def auto_apply():
            await asyncio.sleep(self.apply_timeout)
            if device_id in self.pending_commands and self.pending_commands[device_id]:
                _LOGGER.info(
                    "Auto-applying pending commands for device %s after timeout",
                    device_id,
                )
                await self._execute_pending_commands(device_id)

        self.apply_timers[device_id] = asyncio.create_task(auto_apply())

    async def _command_processor(self):
        """Process commands from the queue."""
        while self.running:
            try:
                command_type, device_id, data = await asyncio.wait_for(
                    self.command_queue.get(), timeout=1.0
                )

                if command_type == "device_command":
                    await self._buffer_device_command(device_id, data)
                elif command_type == "apply_command":
                    await self._handle_apply_command(device_id)

            except asyncio.TimeoutError:
                continue
            except Exception as err:
                _LOGGER.error("Error processing command: %s", err)

    async def _buffer_device_command(
        self, device_id: str, command_data: Dict[str, Any]
    ):
        """Buffer device command for bulk apply."""
        if device_id not in self.devices:
            self._publish_error(f"Unknown device ID: {device_id}")
            return

        try:
            # Initialize pending commands for device if not exists
            if device_id not in self.pending_commands:
                self.pending_commands[device_id] = {}

            # Update pending commands with new data
            self.pending_commands[device_id].update(command_data)

            # Schedule auto-apply after timeout
            await self._schedule_auto_apply(device_id)

            _LOGGER.info(
                "Buffered commands for device %s: %s (pending: %s)",
                device_id,
                command_data,
                self.pending_commands[device_id],
            )

            self._publish_response(
                device_id, True, "Command buffered for bulk apply", command_data
            )

        except Exception as err:
            _LOGGER.error("Error buffering command for device %s: %s", device_id, err)
            self._publish_error(
                f"Error buffering command for device {device_id}: {err}"
            )

    async def _execute_pending_commands(self, device_id: str):
        """Execute all pending commands for a device."""
        if device_id not in self.devices or not self.aux_api:
            self._publish_error(f"Unknown device ID: {device_id}")
            return

        if (
            device_id not in self.pending_commands
            or not self.pending_commands[device_id]
        ):
            _LOGGER.debug("No pending commands for device %s", device_id)
            return

        try:
            await self._ensure_logged_in()

            device = self.devices[device_id]
            pending_data = self.pending_commands[device_id].copy()

            # Send all pending commands in one API call
            await self.aux_api.set_device_params(device, pending_data)

            # Get updated state after applying commands
            updated_state = await self.aux_api.get_device_params(device)
            self._publish_device_state(device_id, updated_state)

            # Clear pending commands
            self.pending_commands[device_id] = {}

            # Cancel auto-apply timer
            if device_id in self.apply_timers:
                self.apply_timers[device_id].cancel()
                del self.apply_timers[device_id]

            _LOGGER.info(
                "Applied pending commands for device %s: %s", device_id, pending_data
            )
            self._publish_response(
                device_id, True, "Pending commands applied successfully", pending_data
            )

        except Exception as err:
            _LOGGER.error(
                "Error applying pending commands for device %s: %s", device_id, err
            )
            self._publish_error(
                f"Error applying pending commands for device {device_id}: {err}"
            )

    async def _handle_apply_command(self, device_id: str):
        """Handle apply command (execute pending commands and refresh state)."""
        if device_id not in self.devices or not self.aux_api:
            self._publish_error(f"Unknown device ID: {device_id}")
            return

        try:
            # Execute any pending commands first
            await self._execute_pending_commands(device_id)

            # Then refresh device state (in case there were no pending commands)
            await self._ensure_logged_in()
            device = self.devices[device_id]
            updated_state = await self.aux_api.get_device_params(device)
            self._publish_device_state(device_id, updated_state)

        except Exception as err:
            _LOGGER.error("Error in apply command for device %s: %s", device_id, err)
            self._publish_error(f"Error in apply command for device {device_id}: {err}")

    async def _ensure_logged_in(self):
        """Ensure API is logged in, re-login if necessary."""
        if not self.aux_api or not self.aux_api.is_logged_in():
            if self.aux_api and self.aux_email and self.aux_password:
                await self.aux_api.login(self.aux_email, self.aux_password)

    async def _update_devices_and_families(self):
        """Update devices and families from the API."""
        try:
            await self._ensure_logged_in()

            if not self.aux_api:
                return False

            families = await self.aux_api.get_families()
            self.families = {family["familyid"]: family for family in families}

            all_devices = {}
            for family_id in self.families:
                devices = await self.aux_api.get_devices(family_id)
                for device in devices:
                    all_devices[device["endpointId"]] = device

            self.devices = all_devices
            _LOGGER.info(
                "Updated %s devices from %s families",
                len(self.devices),
                len(self.families),
            )

            self._publish_device_discovery()
            return True

        except Exception as err:
            _LOGGER.error("Error updating devices and families: %s", err)
            return False

    async def _device_update_loop(self):
        """Periodically update device states."""
        while self.running:
            try:
                await self._ensure_logged_in()

                for device_id, device in self.devices.items():
                    try:
                        if self.aux_api:
                            device_state = await self.aux_api.get_device_params(device)
                            self._publish_device_state(device_id, device_state)
                    except Exception as err:
                        _LOGGER.error("Error updating device %s: %s", device_id, err)

                await asyncio.sleep(self.update_interval)

            except Exception as err:
                _LOGGER.error("Error in device update loop: %s", err)
                await asyncio.sleep(60)

    async def _device_refresh_loop(self):
        """Periodically refresh the device list."""
        while self.running:
            try:
                await asyncio.sleep(self.device_refresh_interval)
                if self.running:
                    await self._update_devices_and_families()
            except Exception as err:
                _LOGGER.error("Error in device refresh loop: %s", err)

    def _publish_device_discovery(self):
        """Publish device discovery information to MQTT."""
        if not self.connected:
            return

        device_list = []
        for device_id, device in self.devices.items():
            param_topics = {}
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

        if self._mqtt_client:
            self._mqtt_client.publish(
                DEVICE_DISCOVERY_TOPIC,
                json.dumps({"devices": device_list}),
                retain=True,
            )

    def _publish_device_state(self, device_id: str, state: Dict[str, Any]):
        """Publish device state to MQTT."""
        if not self.connected or not self._mqtt_client:
            return

        # Publish full state
        topic = STATE_TOPIC_TEMPLATE.format(device_id=device_id)
        self._mqtt_client.publish(topic, json.dumps(state), retain=True)

        # Publish availability
        available_topic = AVAILABLE_TOPIC_TEMPLATE.format(device_id=device_id)
        self._mqtt_client.publish(available_topic, "online", retain=True)

        # Publish individual parameters
        for param, value in state.items():
            param_topic = PARAM_STATE_TOPIC_TEMPLATE.format(
                device_id=device_id, param=param
            )
            formatted_value = (
                json.dumps(value) if isinstance(value, (dict, list)) else str(value)
            )
            self._mqtt_client.publish(param_topic, formatted_value, retain=True)

    def _publish_response(
        self,
        device_id: str,
        success: bool,
        message: str,
        command_data: Optional[Dict[str, Any]] = None,
    ):
        """Publish command response."""
        if not self.connected or not self._mqtt_client:
            return

        response = {
            "device_id": device_id,
            "success": success,
            "message": message,
        }
        if command_data:
            response["command"] = command_data

        self._mqtt_client.publish(COMMAND_RESPONSE_TOPIC, json.dumps(response))

    def _publish_error(self, error_message: str):
        """Publish error message to MQTT."""
        if not self.connected or not self._mqtt_client:
            return

        self._mqtt_client.publish(
            ERROR_TOPIC,
            json.dumps({"timestamp": time.time(), "message": error_message}),
        )

    async def async_stop(self):
        """Stop the bridge."""
        _LOGGER.info("Stopping AUX Cloud MQTT Bridge")
        self.running = False

        # Cancel all tasks
        for task in self.tasks.values():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        # Cancel all apply timers
        for timer in self.apply_timers.values():
            timer.cancel()
        self.apply_timers.clear()

        # Clear pending commands
        self.pending_commands.clear()

        # Set devices offline
        if self.connected and self._mqtt_client:
            for device_id in self.devices:
                offline_topic = AVAILABLE_TOPIC_TEMPLATE.format(device_id=device_id)
                self._mqtt_client.publish(offline_topic, "offline", retain=True)

        # Close connections
        if self.aux_api and hasattr(self.aux_api, "ws_api") and self.aux_api.ws_api:
            try:
                await self.aux_api.ws_api.close_websocket()
            except Exception:
                pass

        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:
                pass


async def load_config(config_path: str):
    """Load configuration from YAML file."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


async def main():
    """Run the AUX Cloud MQTT Bridge."""
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

    # Load configuration
    config_path = os.environ.get("CONFIG_PATH", "aux_cloud_mqtt_config.yaml")
    try:
        config = await load_config(config_path)
    except Exception as err:
        _LOGGER.error("Failed to load configuration: %s", err)
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
        device_refresh_interval=int(
            config.get("settings", {}).get("device_refresh_interval", 3600)
        ),
        enable_websocket=bool(config.get("settings", {}).get("enable_websocket", True)),
        apply_timeout=int(config.get("settings", {}).get("apply_timeout", 30)),
    )

    if not await bridge.async_setup():
        _LOGGER.error("Failed to set up AUX Cloud MQTT Bridge")
        return 1

    # Wait for stop signal
    await stop_event.wait()

    # Stop bridge
    await bridge.async_stop()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
