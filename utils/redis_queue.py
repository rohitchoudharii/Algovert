import redis
import os
import json


class RedisQueue:
    def __init__(self, name, namespace="queue"):
        self.key = f"{namespace}:{name}"
        self.redis = redis.Redis(
            decode_responses=True,
            host=os.environ["REDIS_HOST"],
            port=os.environ["REDIS_PORT"],
        )

    def push(self, item):
        """Push item to the left side of the queue"""
        self.redis.lpush(self.key, json.dumps(item))

    def pop(self):
        """Pop item from the right side (FIFO)"""
        return json.loads(self.redis.rpop(self.key))

    def is_empty(self):
        """Check if the queue is empty"""
        return self.redis.llen(self.key) == 0

    def size(self):
        """Get current queue size"""
        return self.redis.llen(self.key)

    def remove_all(self):
        while not self.is_empty():
            self.redis.rpop(self.key)
