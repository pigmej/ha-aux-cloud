# State Comparison Feature

## Overview

The AUX Cloud MQTT Bridge now includes intelligent state comparison to avoid unnecessary API calls to device controllers. This feature compares the requested device state changes with the current known state and only sends commands that actually change the device's state.

## How It Works

### State Caching
- The bridge maintains a cache of current device states in `self.device_states`
- Device states are updated from multiple sources:
  - WebSocket push notifications from the AUX Cloud service
  - API responses after applying commands
  - Periodic device state polling

### Command Filtering
When a command is received via MQTT, the bridge:

1. **Fetches Current State**: If the device's current state is not cached, it fetches it from the API
2. **Compares Values**: Each requested parameter is compared with the current state
3. **Filters Unchanged**: Only parameters that differ from the current state are included in the command
4. **Skips Empty Commands**: If no parameters need to be changed, the command is skipped entirely

### Value Comparison Logic

The system handles various data types intelligently:

- **Boolean Values**: Converts string representations (`"true"`, `"false"`, `"1"`, `"0"`) to boolean for comparison
- **Numeric Values**: Compares integers and floats with small epsilon tolerance for floating-point precision
- **Strings**: Case-insensitive comparison for string values
- **Null Values**: Properly handles `null`/`None` comparisons

## Benefits

1. **Reduced API Calls**: Eliminates unnecessary requests to the AUX Cloud service
2. **Improved Performance**: Faster response times by avoiding redundant operations
3. **Better Reliability**: Reduces load on the cloud service and potential rate limiting
4. **Cleaner Logs**: Clear indication when no changes are needed

## Example Behavior

### Before (without state comparison):
```
Received command: {"temperature": 22, "fan_speed": "high", "power": true}
→ Always sends all parameters to API
```

### After (with state comparison):
```
Current state: {"temperature": 22, "fan_speed": "high", "power": true, "mode": "cool"}
Received command: {"temperature": 22, "fan_speed": "HIGH", "power": true, "mode": "heat"}
→ Only sends {"mode": "heat"} to API (temperature, fan_speed, power are unchanged)
```

## Logging

The feature includes comprehensive logging at different levels:

- **INFO**: Summary of filtering results and skipped commands
- **DEBUG**: Detailed parameter-by-parameter comparison
- **WARNING**: Issues with fetching current state (falls back to sending all commands)

## Configuration

No additional configuration is required. The feature is automatically enabled and works transparently with existing MQTT commands.

## Implementation Details

### Key Methods

- `_filter_unchanged_commands()`: Main filtering logic
- `_values_differ()`: Value comparison with type handling
- `_to_bool()`: Boolean conversion helper

### State Management

- Device states are stored in `self.device_states` dictionary
- States are updated whenever device information is received
- Cache is maintained across WebSocket updates and API responses

## Edge Cases Handled

1. **Missing Current State**: If current state can't be fetched, all commands are sent
2. **New Parameters**: Parameters not in current state are always sent
3. **Type Mismatches**: Robust type conversion prevents comparison errors
4. **Connection Issues**: Graceful fallback to sending all commands if state fetch fails