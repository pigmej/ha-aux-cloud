#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}AUX Cloud MQTT Bridge Installer${NC}"
echo "=============================="
echo

# Check Python version
python_version=$(python3 --version 2>&1)
if [[ $? -ne 0 ]]; then
  echo -e "${RED}Error: Python 3 is not installed. Please install Python 3.7 or newer.${NC}"
  exit 1
fi

# Extract version number
python_version_num=$(echo $python_version | cut -d' ' -f2)
python_major=$(echo $python_version_num | cut -d'.' -f1)
python_minor=$(echo $python_version_num | cut -d'.' -f2)

if [[ $python_major -lt 3 || ($python_major -eq 3 && $python_minor -lt 7) ]]; then
  echo -e "${RED}Error: Python 3.7 or newer is required. Found: $python_version_num${NC}"
  exit 1
fi

echo -e "${GREEN}Found Python $python_version_num${NC}"

# Create virtual environment if it doesn't exist
if [[ ! -d "venv" ]]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
else
  echo "Virtual environment already exists."
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing required packages..."
pip install -U pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements-mqtt.txt

# If requirements-mqtt.txt exists, use it as a fallback
if [[ -f "requirements-mqtt.txt" ]]; then
  echo "Installing any additional dependencies from requirements-mqtt.txt..."
  pip install -r requirements-mqtt.txt
fi

# Create config file if it doesn't exist
if [[ ! -f "aux_cloud_mqtt_config.yaml" ]]; then
  echo "Creating configuration file..."
  cp aux_cloud_mqtt_config.yaml.example aux_cloud_mqtt_config.yaml
  echo -e "${YELLOW}Please edit aux_cloud_mqtt_config.yaml with your credentials.${NC}"
else
  echo "Configuration file already exists."
fi

# Check if systemd service should be installed
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  echo
  read -p "Do you want to install the systemd service? (y/n) " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Setting up systemd service..."
    
    # Get absolute path to the project directory
    PROJECT_DIR=$(pwd)
    
    # Create systemd service file
    SERVICE_FILE="$PROJECT_DIR/aux-cloud-mqtt.service"
    echo "Creating service file at $SERVICE_FILE..."
    
    # Get current user
        CURRENT_USER=$(whoami)
    
        # Get absolute path to Python in virtualenv
        PYTHON_PATH="$PROJECT_DIR/venv/bin/python"
    
        cat > $SERVICE_FILE << EOF
[Unit]
Description=AUX Cloud MQTT Bridge
After=network.target

[Service]
ExecStart=$PYTHON_PATH $PROJECT_DIR/aux_cloud_mqtt.py
WorkingDirectory=$PROJECT_DIR
Environment=CONFIG_PATH=$PROJECT_DIR/aux_cloud_mqtt_config.yaml
Restart=on-failure
RestartSec=30
User=$CURRENT_USER
Group=$CURRENT_USER

# Optional security enhancements
PrivateTmp=true
ProtectHome=read-only
ProtectSystem=full
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF
    
    echo "Service file created at $SERVICE_FILE"
    echo -e "${YELLOW}To install the service, run:${NC}"
    echo "  sudo cp $SERVICE_FILE /etc/systemd/system/"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable aux-cloud-mqtt.service"
    echo "  sudo systemctl start aux-cloud-mqtt.service"
  fi
fi

echo
echo -e "${GREEN}Installation complete!${NC}"
echo
echo "To run the MQTT bridge:"
echo "  1. Edit aux_cloud_mqtt_config.yaml with your MQTT broker and AUX Cloud credentials"
echo "  2. Activate the virtual environment: source venv/bin/activate"
echo "  3. Run: python aux_cloud_mqtt.py"
echo
echo "For Homey integration, see HOMEY_INTEGRATION.md"
echo