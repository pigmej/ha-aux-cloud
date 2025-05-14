#!/usr/bin/env python3
"""Test script for AUX Cloud MQTT Bridge using individual parameter topics."""

import argparse
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

# MQTT topics
TOPIC_PREFIX = "aux_cloud"
DEVICE_DISCOVERY_TOPIC = f"{TOPIC_PREFIX}/devices"
PARAM_STATE_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/{{param}}"
PARAM_SET_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/{{param}}/set"
AVAILABLE_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/available"
APPLY_TOPIC_TEMPLATE = f"{TOPIC_PREFIX}/{{device_id}}/apply"

# Global variables
devices = {}
selected_device = None
subscribed_topics = set()

def on_connect(client, userdata, flags, rc):
    """Handle MQTT connection."""
    if rc == 0:
        _LOGGER.info("Connected to MQTT broker")
        # Subscribe to discovery topic
        client.subscribe(DEVICE_DISCOVERY_TOPIC)
    else:
        _LOGGER.error("Failed to connect to MQTT broker with code %s", rc)

def on_message(client, userdata, msg):
    """Handle incoming MQTT messages."""
    global devices, selected_device, subscribed_topics
    
    topic = msg.topic
    try:
        payload = msg.payload.decode("utf-8")
        
        if topic == DEVICE_DISCOVERY_TOPIC:
            # Handle device discovery
            try:
                payload_json = json.loads(payload)
                if "devices" in payload_json:
                    devices = {device["id"]: device for device in payload_json["devices"]}
                    _LOGGER.info("Discovered %s devices", len(devices))
                    
                    # Display available devices
                    print("\nAvailable devices:")
                    for i, (device_id, device) in enumerate(devices.items(), 1):
                        online_status = "Online" if device.get("online") else "Offline"
                        print(f"{i}. {device.get('name', 'Unknown')} ({device_id}) - {online_status}")
                    
                    if not selected_device and devices:
                        # Auto-select first device if none is selected
                        selected_device = list(devices.keys())[0]
                        print(f"\nAuto-selected device: {devices[selected_device].get('name', 'Unknown')} ({selected_device})")
                        
                        # Subscribe to selected device parameters
                        subscribe_to_device(client, selected_device)
            except json.JSONDecodeError:
                _LOGGER.error("Invalid JSON in discovery topic")
        elif selected_device and topic.startswith(f"{TOPIC_PREFIX}/{selected_device}/"):
            # Parameter update
            parts = topic.split("/")
            if len(parts) == 3:  # Should be aux_cloud/device_id/param
                param = parts[2]
                print(f"\nParameter {param} updated to: {payload}")
    except Exception as err:
        _LOGGER.error("Error handling message: %s", err)

def subscribe_to_device(client, device_id):
    """Subscribe to all parameters for a device."""
    global subscribed_topics
    
    if device_id not in devices:
        print(f"Device {device_id} not found")
        return
        
    # Unsubscribe from previous device's topics
    for topic in subscribed_topics:
        client.unsubscribe(topic)
    subscribed_topics.clear()
    
    # Subscribe to availability
    available_topic = AVAILABLE_TOPIC_TEMPLATE.format(device_id=device_id)
    client.subscribe(available_topic)
    subscribed_topics.add(available_topic)
    
    # Subscribe to all parameters
    device = devices[device_id]
    if "topics" in device and "parameters" in device["topics"]:
        for param, topics in device["topics"]["parameters"].items():
            state_topic = topics.get("state")
            if state_topic:
                client.subscribe(state_topic)
                subscribed_topics.add(state_topic)
                _LOGGER.info(f"Subscribed to {state_topic}")
    else:
        # Subscribe to common parameters
        common_params = ["power", "mode", "temperature", "fan_speed", "current_temperature"]
        for param in common_params:
            topic = PARAM_STATE_TOPIC_TEMPLATE.format(device_id=device_id, param=param)
            client.subscribe(topic)
            subscribed_topics.add(topic)
            _LOGGER.info(f"Subscribed to {topic}")
    
    print(f"Subscribed to topics for device {device_id}")

def send_parameter(client, device_id, param, value):
    """Send parameter value to device."""
    if device_id not in devices:
        _LOGGER.error("Unknown device ID: %s", device_id)
        return
        
    topic = PARAM_SET_TOPIC_TEMPLATE.format(device_id=device_id, param=param)
    _LOGGER.info("Sending %s=%s to %s", param, value, topic)
    client.publish(topic, str(value))
    
    print(f"Parameter {param} set to {value}")
    print("Tip: Use 'Apply changes & refresh device' to apply immediately")

def main():
    """Run the MQTT parameter test client."""
    parser = argparse.ArgumentParser(description='Test AUX Cloud MQTT Bridge with individual parameters.')
    parser.add_argument('--host', default='localhost', help='MQTT broker host')
    parser.add_argument('--port', type=int, default=1883, help='MQTT broker port')
    parser.add_argument('--username', help='MQTT username')
    parser.add_argument('--password', help='MQTT password')
    
    args = parser.parse_args()
    
    # Initialize MQTT client
    client = mqtt.Client(
        client_id="aux_cloud_param_test",
        protocol=mqtt.MQTTv311
    )
    
    if args.username and args.password:
        client.username_pw_set(args.username, args.password)
        
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        # Connect to MQTT broker
        _LOGGER.info("Connecting to MQTT broker at %s:%s", args.host, args.port)
        client.connect(args.host, args.port)
        client.loop_start()
        
        # Wait a bit for initial connection and device discovery
        time.sleep(2)
        
        # Interactive menu
        while True:
            print("\n=== AUX Cloud MQTT Parameter Test ===")
            if selected_device:
                device_name = devices.get(selected_device, {}).get("name", "Unknown")
                print(f"Selected device: {device_name} ({selected_device})")
                
            print("\nOptions:")
            print("1. List devices")
            print("2. Select device")
            print("3. Turn power ON")
            print("4. Turn power OFF")
            print("5. Set temperature")
            print("6. Set mode (cool, heat, auto, dry, fan)")
            print("7. Set fan speed (auto, high, medium, low)")
            print("8. Custom parameter")
            print("9. Apply changes & refresh device")
            print("0. Exit")
            
            choice = input("\nEnter choice (0-9): ")
            
            if choice == "0":
                break
                
            elif choice == "1":
                # Force refresh device list
                client.publish(DEVICE_DISCOVERY_TOPIC, "", retain=False)
                time.sleep(1)  # Wait for response
                
            elif choice == "2":
                # Select device
                if not devices:
                    print("No devices discovered yet.")
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
                        subscribe_to_device(client, selected_device)
                    else:
                        print("Invalid device number")
                except ValueError:
                    print("Invalid input. Please enter a number.")
                    
            elif choice == "3":
                # Turn ON
                if not selected_device:
                    print("No device selected")
                    continue
                    
                send_parameter(client, selected_device, "power", "on")
                
            elif choice == "4":
                # Turn OFF
                if not selected_device:
                    print("No device selected")
                    continue
                    
                send_parameter(client, selected_device, "power", "off")
                
            elif choice == "5":
                # Set temperature
                if not selected_device:
                    print("No device selected")
                    continue
                    
                try:
                    temp = int(input("Enter temperature (16-30): "))
                    if 16 <= temp <= 30:
                        send_parameter(client, selected_device, "temperature", temp)
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
                    send_parameter(client, selected_device, "mode", mode_map[mode_choice])
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
                    send_parameter(client, selected_device, "fan_speed", fan_map[fan_choice])
                else:
                    print("Invalid fan speed choice")
                    
            elif choice == "8":
                # Custom parameter
                if not selected_device:
                    print("No device selected")
                    continue
                    
                param = input("Enter parameter name: ")
                value = input("Enter value: ")
                
                send_parameter(client, selected_device, param, value)
                
            elif choice == "9":
                # Apply changes and refresh device
                if not selected_device:
                    print("No device selected")
                    continue
                
                topic = APPLY_TOPIC_TEMPLATE.format(device_id=selected_device)
                _LOGGER.info(f"Sending apply command to {topic}")
                client.publish(topic, "true")
                print(f"Apply command sent to device {selected_device}")
                
            else:
                print("Invalid choice")
                
            # Small delay to allow MQTT messages to be processed
            time.sleep(1)
            
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