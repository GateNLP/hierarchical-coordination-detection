# Centralise redis connection configuration in one location that can be shared
# by the Flask app and the workers

import os
import redis

redis_kwargs = dict(
    host=os.environ.get('REDIS_HOST', 'redis'),
    port=int(os.environ.get('REDIS_PORT', '6379')),
    db=0,
)

if "REDIS_USERNAME" in os.environ:
    redis_kwargs["username"] = os.environ["REDIS_USERNAME"]
if "REDIS_PASSWORD" in os.environ:
    redis_kwargs["password"] = os.environ["REDIS_PASSWORD"]

# TODO support for secure connections

redis_connection = redis.Redis(**redis_kwargs)

