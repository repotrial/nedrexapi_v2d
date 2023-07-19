#!/bin/bash

export NEDREX_CONFIG=".open_config.toml"
# Clear the redis cache
./clear_redis.py --host open-nedrex-redis --port 6379 -d 1 -d 2 -d 3
# Run the API
gunicorn nedrexapi.main:app -b 0.0.0.0:7123 -w 10 -k uvicorn.workers.UvicornWorker --timeout 120 --access-logfile -
