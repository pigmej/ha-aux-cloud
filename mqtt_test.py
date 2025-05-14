#!/usr/bin/env python3
"""Test script for AUX Cloud MQTT Bridge."""
import asyncio
import json
import logging
import sys
import time
import paho.mqtt.client as mqtt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
_LOGGER = logging.getLogger(__name__)

# MQTT settings
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
MQTT_CLIENT_ID = "aux_cloud_mqtt_test"

# MQTT topics
TOPIC_PREFIX = "aux_cloud"
DEVICE_DISCOVERY_TOPIC = f"{TOPIC_PREFIX}/devices"
SET_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/set"
STATE_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/state"
COMMAND_RESPONSE_TOPIC = f"{TOPIC_PREFIX}/response"
ERROR_TOPIC = f"{TOPIC_PREFIX}/error"

# Global variables
devices = {}
selected_device = None

def on_connect(client, userdata, flags, rc):
    """Handle MQTT connection."""
    if rc == 0:
        _LOGGER.info("Connected to MQTT broker")
        # Subscribe to all relevant topics
        client.subscribe(DEVICE_DISCOVERY_TOPIC)
        client.subscribe(f"{TOPIC_PREFIX}/+/state")
        client.subscribe(COMMAND_RESPONSE_TOPIC)
        client.subscribe(ERROR_TOPIC)
    else:
        _LOGGER.error("Failed to connect to MQTT broker with code %s", rc)

def on_message(client, userdata, msg):
    """Handle incoming MQTT messages."""
    global devices, selected_device
    
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        _LOGGER.warning("Received non-JSON payload on topic %s", topic)
        return
    
    if topic == DEVICE_DISCOVERY_TOPIC:
        # Handle device discovery
        if "devices" in payload:
            devices = {device["id"]: device for device in payload["devices"]}
            _LOGGER.info("Discovered %s devices", len(devices))
            
            # Display available devices
            print("\nAvailable devices:")
            for i, (device_id, device) in enumerate(devices.items(), 1):
                print(f"{i}. {device.get('name', 'Unknown')} ({device_id}) - {'Online' if device.get('online') else 'Offline'}")
            
            if not selected_device and devices:
                # Auto-select first device if none is selected
                selected_device = list(devices.keys())[0]
                print(f"\nAuto-selected device: {devices[selected_device].get('name', 'Unknown')} ({selected_device})")
                
    elif topic.endswith("/state"):
        # Device state update
        device_id = topic.split("/")[1]
        if device_id == selected_device:
            print("\nDevice state updated:")
            print(json.dumps(payload, indent=2))
            
    elif topic == COMMAND_RESPONSE_TOPIC:
        # Command response
        print("\nCommand response:")
        print(json.dumps(payload, indent=2))
        
    elif topic == ERROR_TOPIC:
        # Error message
        _LOGGER.error("Error from bridge: %s", payload.get("message", "Unknown error"))

def send_command(client, device_id, command):
    """Send command to device."""
    if device_id not in devices:
        _LOGGER.error("Unknown device ID: %s", device_id)
        return
        
    topic = SET_TOPIC_TEMPLATE.format(device_id=device_id)
    _LOGGER.info("Sending command to %s: %s", topic, command)
    client.publish(topic, json.dumps(command))

def interactive_menu(client):
    """Display interactive menu."""
    global selected_device
    
    while True:
        print("\n=== AUX Cloud MQTT Test Client ===")
        if selected_device:
            device_name = devices.get(selected_device, {}).get("name", "Unknown")
            print(f"Selected device: {device_name} ({selected_device})")
            
        print("\nOptions:")
        print("1. List devices")
        print("2. Select device")
        print("3. Turn device ON")
        print("4. Turn device OFF")
        print("5. Set temperature")
        print("6. Set mode (cool, heat, auto, dry, fan)")
        print("7. Set fan speed (auto, high, medium, low)")
        print("8. Custom command")
        print("0. Exit")
        
        choice = input("\nEnter choice (0-8): ")
        
        if choice == "0":
            return
            
        elif choice == "1":
            # List devices
            if not devices:
                print("No devices discovered yet. Waiting for discovery...")
                continue
                
            print("\nAvailable devices:")
            for i, (device_id, device) in enumerate(devices.items(), 1):
                print(f"{i}. {device.get('name', 'Unknown')} ({device_id}) - {'Online' if device.get('online') else 'Offline'}")
                
        elif choice == "2":
            # Select device
            if not devices:
                print("No devices discovered yet. Waiting for discovery...")
                continue
                
            print("\nAvailable devices:")
            for i, (device_id, device) in enumerate(devices.items(), 1):
                print(f"{i}. {device.get('name', 'Unknown')} ({device_id})")
                
            try:
                device_index = int(input("\nEnter device number: ")) - 1
                device_ids = list(devices.keys())
                if 0 <= device_index < len(device_ids):
                    selected_device = device_ids[device_index]
                    print(f"Selected device: {devices[selected_device].get('name', 'Unknown')} ({selected_device})")
                else:
                    print("Invalid device number")
            except ValueError:
                print("Invalid input. Please enter a number.")
                
        elif choice == "3":
            # Turn ON
            if not selected_device:
                print("No device selected")
                continue
                
            send_command(client, selected_device, {"power": "on"})
            
        elif choice == "4":
            # Turn OFF
            if not selected_device:
                print("No device selected")
                continue
                
            send_command(client, selected_device, {"power": "off"})
            
        elif choice == "5":
            # Set temperature
            if not selected_device:
                print("No device selected")
                continue
                
            try:
                temp = int(input("Enter temperature (16-30): "))
                if 16 <= temp <= 30:
                    send_command(client, selected_device, {"temperature": temp})
                else:
                    print("Temperature must be between 16 and 30")
            except ValueError:
                print("Invalid input. Please enter a number.")
                
        elif choice == "6":
            # Set mode
            if not selected_device:
                print("No device selected")
                continue
                
            print("\nAvailable modes:")
            print("1. Cool")
            print("2. Heat")
            print("3. Auto")
            print("4. Dry")
            print("5. Fan")
            
            mode_map = {
                "1": "cool",
                "2": "heat",
                "3": "auto",
                "4": "dry",
                "5": "fan"
            }
            
            mode_choice = input("Enter mode (1-5): ")
            if mode_choice in mode_map:
                send_command(client, selected_device, {"mode": mode_map[mode_choice]})
            else:
                print("Invalid mode choice")
                
        elif choice == "7":
            # Set fan speed
            if not selected_device:
                print("No device selected")
                continue
                
            print("\nAvailable fan speeds:")
            print("1. Auto")
            print("2. High")
            print("3. Medium")
            print("4. Low")
            
            fan_map = {
                "1": "auto",
                "2": "high",
                "3": "medium",
                "4": "low"
            }
            
            fan_choice = input("Enter fan speed (1-4): ")
            if fan_choice in fan_map:
                send_command(client, selected_device, {"fan_speed": fan_map[fan_choice]})
            else:
                print("Invalid fan speed choice")
                
        elif choice == "8":
            # Custom command
            if not selected_device:
                print("No device selected")
                continue
                
            print("Enter custom command as JSON (e.g., {\"power\": \"on\", \"temperature\": 22})")
            command_str = input("Command: ")
            
            try:
                command = json.loads(command_str)
                send_command(client, selected_device, command)
            except json.JSONDecodeError:
                print("Invalid JSON format")
                
        else:
            print("Invalid choice")
            
        # Small delay to allow MQTT messages to be processed
        time.sleep(1)

def main():
    """Run the MQTT test client."""
    # Create MQTT client
    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311
    )
    _LOGGER.info("MQTT client initialized")
    
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        _LOGGER.info("Connecting to MQTT broker at %s:%s", MQTT_HOST, MQTT_PORT)
        client.connect(MQTT_HOST, MQTT_PORT)
        client.loop_start()
        
        # Wait a bit for initial connection and device discovery
        time.sleep(2)
        
        # Start interactive menu
        interactive_menu(client)
        
    except KeyboardInterrupt:
        _LOGGER.info("Test client stopped by user")
    except Exception as err:
        _LOGGER.error("Error: %s", err)
    finally:
        client.loop_stop()
        client.disconnect()
        
    return 0

if __name__ == "__main__":
    sys.exit(main())