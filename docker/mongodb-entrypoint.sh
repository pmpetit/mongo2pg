#!/bin/sh
set -eu

printf '%s' "${MONGO_REPLICA_SET_KEY}" > /tmp/mongodb-keyfile
chmod 600 /tmp/mongodb-keyfile

exec python3 /usr/local/bin/docker-entrypoint.py "$@"
