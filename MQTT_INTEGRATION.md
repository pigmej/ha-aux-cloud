# AUX Cloud MQTT Bridge

This is an MQTT bridge for AUX Cloud connected appliances that allows you to integrate your AUX devices with Homey or other MQTT-compatible smart home platforms.

## Features

- Control AUX air conditioners and heat pumps via MQTT
- Automatic device discovery
- Real-time state updates via polling and WebSocket
- Command response and error reporting
- Compatible with Homey Pro through the MQTT app
- Instant state updates using WebSocket connection (optional)

## Installation

### Requirements

- Python 3.7 or newer
- `paho-mqtt` Python library (version 2.0.0 or newer)
- `pyyaml` Python library
- `aiohttp` Python library

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/ha-aux-cloud.git
   cd ha-aux-cloud
   ```

2. Install required Python packages:
   ```bash
   # The easy way - using the install script
   chmod +x install_mqtt_bridge.sh
   ./install_mqtt_bridge.sh
   ```

   Or manually:
   ```bash
   pip install paho-mqtt>=2.0.0 pyyaml aiohttp
   ```

3. Edit the configuration file:
   ```bash
   cp aux_cloud_mqtt_config.yaml.example aux_cloud_mqtt_config.yaml
   nano aux_cloud_mqtt_config.yaml
   ```

4. Update the configuration with your MQTT broker and AUX Cloud credentials.

## Running the Bridge

Start the bridge service:

```bash
python aux_cloud_mqtt.py
```

To run the service in the background:

```bash
nohup python aux_cloud_mqtt.py > aux_cloud_mqtt.log 2>&1 &
```

## MQTT Topics

The bridge uses the following MQTT topics:

| Topic | Description |
|-------|-------------|
| `aux_cloud/devices` | Device discovery information (retained) |
| `aux_cloud/<device_id>/state` | Current state of the device as JSON (retained) |
| `aux_cloud/<device_id>/set` | Send commands to the device as JSON |
| `aux_cloud/<device_id>/<param>` | Individual parameter state (retained) |
| `aux_cloud/<device_id>/<param>/set` | Set individual parameter value |
| `aux_cloud/<device_id>/available` | Device availability status (online/offline) |
| `aux_cloud/<device_id>/apply` | Force immediate (real-time) application of pending changes and refresh device state |
| `aux_cloud/response` | Command execution response |
| `aux_cloud/error` | Error messages |

When the WebSocket connection is enabled, device states will be updated in real-time when changes are made through the official AUX app or other integrations, providing faster feedback than the polling interval. The bridge automatically monitors and reconnects the WebSocket if the connection is lost.

### Device Discovery

The device discovery topic (`aux_cloud/devices`) provides a list of all available devices with their IDs and topics. This is published when the bridge starts and whenever devices are updated.

Example payload:
```json
{
  "devices": [
    {
      "id": "device123",
      "name": "Living Room AC",
      "type": "air_conditioner",
      "online": true,
      "family_id": "family456",
      "topics": {
        "set": "aux_cloud/device123/set",
        "state": "aux_cloud/device123/state"
      }
    }
  ]
}
```

### Device State

Each device publishes its state in two ways:

1. Complete state as JSON to `aux_cloud/<device_id>/state`
2. Individual parameters to `aux_cloud/<device_id>/<param>` (one topic per parameter)

The individual parameter topics make it easier to integrate with systems like Homey that prefer simple values rather than complex JSON objects.

### Controlling Devices

There are two ways to control a device:

#### 1. Full JSON Command

Publish a JSON object to `aux_cloud/<device_id>/set` with the parameters you want to set:

```json
{
  "power": "on",
  "mode": "cool",
  "temperature": 22,
  "fan_speed": "auto"
}
```

#### 2. Individual Parameter Commands

Publish a single value to `aux_cloud/<device_id>/<param>/set`. For example:

- To set temperature: Publish `22` to `aux_cloud/device123/temperature/set`
- To turn on: Publish `on` to `aux_cloud/device123/power/set`
- To change mode: Publish `cool` to `aux_cloud/device123/mode/set`

This approach is often easier for integration with automation platforms.

#### 3. Apply Command (Real-time Execution)

To force immediate, real-time application of any pending changes and refresh device state:

- Publish `true` to `aux_cloud/<device_id>/apply`

This command executes immediately (not queued) to provide instant feedback. Unlike other commands, it bypasses the normal command queue for real-time response. This is essential when you need instantaneous updates or when synchronizing with changes made through other means (like the AUX app).

## Integrating with Homey

To use this bridge with Homey:

1. Install the MQTT app on your Homey Pro from the Homey App Store.

2. Configure the MQTT app to connect to the same MQTT broker as your bridge.

3. Create virtual devices in Homey that subscribe to the individual parameter topics:
   - Use `aux_cloud/<device_id>/<param>` for reading states
   - Use `aux_cloud/<device_id>/<param>/set` for sending commands
   - Use `aux_cloud/<device_id>/available` for availability status

4. Use Homey Flows to automate your AUX devices.

For detailed instructions on setting up Homey devices, see the HOMEY_INTEGRATION.md file.

## Troubleshooting

### Common Issues

- **Bridge not connecting to MQTT broker**: Check your MQTT credentials and broker address.
- **Bridge not connecting to AUX Cloud**: Verify your AUX Cloud email, password, and region.
- **Commands not working**: Ensure the device is online and check the error topic for messages.
- **WebSocket connection failing**: The WebSocket may be blocked by a firewall. Set `enable_websocket: false` in config if needed.
- **MQTT client errors**: The bridge requires paho-mqtt 2.0.0 or newer. If you encounter issues, try installing the latest version: `pip install --upgrade paho-mqtt`.
- **Commands seem delayed**: Use the apply command to force real-time updates - publish `true` to `aux_cloud/<device_id>/apply`. This command executes immediately without queuing.

### Logs

Check the logs for detailed error messages:

```bash
tail -f aux_cloud_mqtt.log
```

To enable more detailed debugging, set the `log_level` to `DEBUG` in your configuration file.

```bash
# Show MQTT debug output
PYTHONPATH=. python -m paho.mqtt.client -d -h localhost

# Run with debug logging
python aux_cloud_mqtt.py
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `mqtt.host` | MQTT broker address | `localhost` |
| `mqtt.port` | MQTT broker port | `1883` |
| `mqtt.username` | MQTT username | `null` |
| `mqtt.password` | MQTT password | `null` |
| `mqtt.client_id` | MQTT client ID | `aux_cloud_mqtt` |
| `aux_cloud.email` | AUX Cloud account email | Required |
| `aux_cloud.password` | AUX Cloud account password | Required |
| `aux_cloud.region` | AUX Cloud region (eu, usa, cn) - case insensitive | `eu` |
| `settings.enable_websocket` | Enable WebSocket for real-time updates | `true` |
| `settings.update_interval` | Time in seconds between device state updates | `60` |
| `settings.log_level` | Logging level | `INFO` |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
