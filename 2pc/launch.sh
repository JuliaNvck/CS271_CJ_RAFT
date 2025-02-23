#!/bin/bash

# Get the current working directory
current_dir=$(pwd)

# Read the config.json file using Python
config_file="config.json"
coordinator_ip=$(python3 -c "import json; f=open('$config_file'); data=json.load(f); print(data['coordinator']['ip']); f.close()")
coordinator_port=$(python3 -c "import json; f=open('$config_file'); data=json.load(f); print(data['coordinator']['port']); f.close()")
servers=$(python3 -c "import json; f=open('$config_file'); data=json.load(f); print('\n'.join([json.dumps(server) for server in data['servers']])); f.close()")

# Launch the coordinator (Client) in a new Terminal window
osascript <<EOF
tell application "Terminal"
    do script "cd '$current_dir' && python3 client.py $coordinator_port"
end tell
EOF

# Wait for the coordinator to start (optional, adjust sleep time as needed)
sleep 2

# Group servers by cluster
declare -A clusters
while IFS= read -r server; do
    cluster=$(echo "$server" | python3 -c "import json, sys; print(json.load(sys.stdin)['cluster'])")
    ip=$(echo "$server" | python3 -c "import json, sys; print(json.load(sys.stdin)['ip'])")
    port=$(echo "$server" | python3 -c "import json, sys; print(json.load(sys.stdin)['port'])")
    id=$(echo "$server" | python3 -c "import json, sys; print(json.load(sys.stdin)['id'])")
    clusters["$cluster"]+="$ip:$port:$id"$'\n'
done <<< "$servers"

# Launch each cluster in a new Terminal window with tabs for each server
for cluster in "${!clusters[@]}"; do
    IFS=$'\n' read -r -d '' -a servers_in_cluster <<< "${clusters[$cluster]}"
    first_server=1
    for server in "${servers_in_cluster[@]}"; do
        IFS=':' read -r ip port id <<< "$server"
        if [ "$first_server" -eq 1 ]; then
            # Open a new Terminal window for the first server in the cluster
            osascript <<EOF
tell application "Terminal"
    do script "cd '$current_dir' && python3 server.py $port $id $cluster"
end tell
EOF
            first_server=0
        else
            # Open a new tab for the remaining servers in the cluster
            osascript <<EOF
tell application "Terminal"
    activate
    tell application "System Events" to tell process "Terminal" to keystroke "t" using command down
    do script "cd '$current_dir' && python3 server.py $port $id $cluster" in front window
end tell
EOF
        fi
    done
done

echo "Success."