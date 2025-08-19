#!/bin/bash

# Create necessary directories
mkdir -p ~/.streamlit/

# Set Streamlit configuration
echo "\n[server]\nheadless = true\nport = $PORT\nenableCORS = false\n\n[browser]\ngatherUsageStats = false\nserverAddress = '0.0.0.0'\n\n[theme]\nprimaryColor='#FF4B4B'\nbackgroundColor='#0E1117'\nsecondaryBackgroundColor='#1E1E1E'\ntextColor='#FAFAFA'\nfont='sans serif'\n" > ~/.streamlit/config.toml

# Set environment variables
export PYTHONPATH=$PYTHONPATH:$(pwd)
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
