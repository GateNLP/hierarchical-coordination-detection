#!/bin/sh

# Allow for setup shell scripts to be injected to set up environment variables
# etc. before we execute the main process
for f in setup.d/*.sh ; do
  if [ -r "$f" ]; then
    . "$f"
  fi
done

if [ "$1" = "worker" ]; then
  exec /usr/bin/tini -- venv/bin/python coordination_worker.py
elif [ "$1" = "cleanup" ]; then
  exec /usr/bin/tini -- venv/bin/python cleanup.py
else
  exec /usr/bin/tini -- venv/bin/gunicorn -b=0.0.0.0:$WEBAPP_PORT "--workers=$WORKERS" "$@" coordination.webapp:app
fi
