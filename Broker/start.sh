#!/bin/sh
export MANAGER_ADDRESS="http://127.0.0.1:8000"
export CURRENT_ADDRESS="http://127.0.0.1:$PORT"
python3 app/main.py $PORT