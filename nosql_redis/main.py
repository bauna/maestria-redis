import os
from redis import Redis

async def redis_set(key: str | int, value: str | int):
  r = Redis(host=os.environ["REDIS_HOST"], port=int(os.environ["REDIS_PORT"]), decode_responses=True)
  r.set(key, value)
