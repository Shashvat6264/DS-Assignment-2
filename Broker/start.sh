#!/bin/sh
# export MANAGER_ADDRESS="http://127.0.0.1:8000"
host=$(hostname -I)
export CURRENT_ADDRESS="http://$host:$PORT"
python3 app/main.py $PORT