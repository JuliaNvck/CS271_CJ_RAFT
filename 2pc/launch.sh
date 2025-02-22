#!/bin/bash

# Define the ports for the coordinator and servers
coordinator_port=5000
server_ports=(5001 5002 5003)

# Get the current working directory
current_dir=$(pwd)

# Launch the coordinator in a new Terminal window
osascript <<EOF
tell application "Terminal"
    do script "cd '$current_dir' && python3 main.py $coordinator_port"
end tell
EOF

# Wait for the coordinator to start (optional, adjust sleep time as needed)
sleep 2

# Launch the first server in a new Terminal window
osascript <<EOF
tell application "Terminal"
    do script "cd '$current_dir' && python3 main.py ${server_ports[0]}"
end tell
EOF

# Loop through remaining server ports and open them in new tabs
for port in "${server_ports[@]:1}"; do
    osascript <<EOF
tell application "Terminal"
    activate
    tell application "System Events" to tell process "Terminal" to keystroke "t" using command down
    do script "cd '$current_dir' && python3 main.py $port" in front window
end tell
EOF
done

echo "Coordinator and servers launched successfully."