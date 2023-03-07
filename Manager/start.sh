#!/bin/sh
while getopts r flag
do
    case "${flag}" in
        r) export READ_ONLY=True;;
    esac
done

python3 app/main.py $PORT