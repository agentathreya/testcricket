#!/bin/bash
mkdir -p ~/.streamlit/
echo "\n[server]\nheadless = true\nport = $PORT\nenableCORS = false\n\n[browser]\ngatherUsageStats = false\n\n[theme]\nprimaryColor='#FF4B4B'\nbackgroundColor='#0E1117'\nsecondaryBackgroundColor='#1E1E1E'\ntextColor='#FAFAFA'\nfont='sans serif'\n" > ~/.streamlit/config.toml
