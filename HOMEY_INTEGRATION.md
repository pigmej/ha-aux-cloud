# Integrating AUX Cloud with Homey Pro

This guide explains how to set up the AUX Cloud MQTT Bridge with Homey Pro to control your AUX devices directly from your Homey smart home system.

## Prerequisites

1. AUX Cloud MQTT Bridge installed and running
2. Homey Pro with MQTT app installed
3. Access to Homey Web Interface

## Setup Process

### 1. Install MQTT App in Homey

1. Go to the Homey Web Interface
2. Navigate to Apps > Add App
3. Search for "MQTT"
4. Install the "MQTT Client" app by Athom

### 2. Configure MQTT Connection in Homey

1. Open the MQTT Client app settings
2. Configure the MQTT broker connection:
   - Broker Address: (same as in your `aux_cloud_mqtt_config.yaml`)
   - Port: (same as in your `aux_cloud_mqtt_config.yaml`)
   - Username & Password: (if configured)
   - Client ID: `homey_mqtt_client` (or any unique identifier)

### 3. Create Virtual Devices in Homey

There are two ways to create devices in Homey:
1. Using the JSON state topic (traditional approach)
2. Using individual parameter topics (recommended, simpler approach)

#### Method 1: Create a Thermostat Device with Individual Parameter Topics (Recommended)

1. In Homey, go to Devices > Add Device
2. Select "MQTT Client" app
3. Choose "Generic MQTT Device"
4. Enter the following basic configuration:
   - Name: "Living Room AC" (or your preferred name)
   - Availability topic: `aux_cloud/device123/available` (replace device123 with your actual device ID)
   - Payload online: `online`
   - Payload offline: `offline`
   - Capabilities:
     - Target Temperature (`target_temperature`)
     - Measure Temperature (`measure_temperature`)
     - Thermostat Mode (`thermostat_mode`)
     - On/Off (`onoff`)

5. Configure each capability separately using individual topics:
   - For On/Off:
     - Subscribe topic: `aux_cloud/device123/power`
     - Publish topic: `aux_cloud/device123/power/set`
     - Payload on: `on`
     - Payload off: `off`
   - For Target Temperature:
     - Subscribe topic: `aux_cloud/device123/temperature`
     - Publish topic: `aux_cloud/device123/temperature/set`
     - Min value: 16
     - Max value: 30
   - For Measure Temperature:
     - Subscribe topic: `aux_cloud/device123/current_temperature`
     - (no publish topic needed as this is read-only)
   - For Thermostat Mode:
     - Subscribe topic: `aux_cloud/device123/mode`
     - Publish topic: `aux_cloud/device123/mode/set`
     - Values:
       - heat
       - cool
       - auto
       - off

#### Method 2: Create a Fan Device with Individual Parameter Topics

1. In Homey, go to Devices > Add Device
2. Select "MQTT Client" app
3. Choose "Generic MQTT Device"
4. Enter the following configuration:
   - Name: "Living Room AC Fan"
   - Availability topic: `aux_cloud/device123/available`
   - Payload online: `online`
   - Payload offline: `offline`
   - Capabilities:
     - Fan Speed (`fan_speed`)
5. Configure Fan Speed:
   - Subscribe topic: `aux_cloud/device123/fan_speed`
   - Publish topic: `aux_cloud/device123/fan_speed/set`
   - Values:
     - low
     - medium
     - high
     - auto

### 4. Create Flows in Homey

#### Example Flow: Auto Mode when Coming Home

1. Go to Flows > Create new Flow
2. Trigger: When "Presence" changes to "Home"
3. Action:
   - Device: Your AUX AC device
   - Action: Set Thermostat Mode to "auto"
   - Action: Set Target Temperature to 22°C

#### Example Flow: Voice Control

1. Go to Flows > Create new Flow
2. Trigger: When voice command "Set living room temperature to X degrees" is recognized
3. Action:
   - Device: Your AUX AC device
   - Action: Set Target Temperature to X°C

## Advanced Integration

### Creating a Combined Device Card

For a better experience, you can create a custom card in Homey that combines all controls for your AUX device:

1. Go to More > Developer > Tools > Create Device
2. Configure with all the capabilities:
   - On/Off
   - Target Temperature
   - Measure Temperature
   - Thermostat Mode
   - Fan Speed
3. Create flow logic to translate between this virtual device and your MQTT device

## Troubleshooting

### Device State Not Updating

If your device state isn't updating in Homey:

1. Check that the MQTT bridge is running properly
2. Verify that topics match exactly
3. Check the MQTT Bridge logs for errors
4. Use an MQTT Explorer to verify messages are being published
5. Make sure the device is online in the AUX app

### Commands Not Working

If commands from Homey aren't working:

1. Verify you're publishing to the correct topic
2. For individual parameter topics, make sure you're sending simple values, not JSON
3. Check the MQTT Bridge logs for errors
4. Verify the device is showing as "online" in the availability topic

## Command Examples

### JSON Commands (for traditional approach)

```json
{"power": "on", "temperature": 22, "mode": "cool"}
```

```json
{"fan_speed": "auto"}
```

```json
{"power": "off"}
```

### Individual Parameter Commands (for recommended approach)

- Set power: Publish `on` to `aux_cloud/device123/power/set`
- Set temperature: Publish `22` to `aux_cloud/device123/temperature/set`
- Set mode: Publish `cool` to `aux_cloud/device123/mode/set`
- Set fan speed: Publish `auto` to `aux_cloud/device123/fan_speed/set`

## Additional Resources

- [Homey MQTT Client Documentation](https://apps.athom.com/app/nl.scanno.mqtt)
- [MQTT Explorer](http://mqtt-explorer.com/) (useful debugging tool)