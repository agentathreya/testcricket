#!/bin/bash
set -e

# Ensure we're using Python 3.11
python3.11 --version || (echo "Python 3.11 not found" && exit 1)

# Create and activate virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Upgrade pip and install requirements
python -m pip install --upgrade pip
pip install -r requirements.txt

# Make setup script executable
chmod +x setup.sh
