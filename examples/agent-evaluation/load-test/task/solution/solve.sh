#!/bin/sh
set -eu

python3 /opt/eruun-load/simulate.py /app/load-timing.json
cp /app/load-timing.json /logs/artifacts/load-timing.json
