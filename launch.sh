#!/bin/bash

# Define the ports for the servers
ports=(5000 5001 5002)

# Get the current working directory
current_dir=$(pwd)

# Open a new Terminal window and run the first command
osascript -e "tell application \"Terminal\" to do script \"cd '$current_dir' && python3 bank.py ${ports[0]}\"" 

# Loop through remaining ports and open them in new tabs
for port in "${ports[@]:1}"; do
    osascript -e "tell application \"Terminal\" to activate" \
              -e "tell application \"System Events\" to tell process \"Terminal\" to keystroke \"t\" using command down" \
              -e "tell application \"Terminal\" to do script \"cd '$current_dir' && python3 bank.py $port\" in front window"
done

echo "Servers launched successfully in separate tabs."
