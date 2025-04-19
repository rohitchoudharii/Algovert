from feeds import BaseFeed, FeedData
from utils.redis_queue import RedisQueue
from datetime import datetime


class OHLCQueueFeed(BaseFeed):
    def __init__(self, queue: RedisQueue, **configs):
        super().__init__(**configs)
        self.queue = queue

    def next(self, data: FeedData) -> FeedData:
        if not self.queue.is_empty():
            queue_data = self.queue.pop()
            # print(queue_data)
            dt = datetime.fromtimestamp(queue_data["epoch"])
            ltp = round(float(queue_data["ltp"]), 2)

            return FeedData(dt, ltp, ltp, ltp, ltp, 0, self.name)


class OHLCDataBaseFeed(BaseFeed):
    pass
