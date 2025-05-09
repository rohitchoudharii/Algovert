from feeds import BaseFeed, FeedData
from feeds.resampler_feed import ResampleFeed
from feeds.aggregator_feed import AggregatorFeed
from feeds.ohlc_feed import OHLCQueueFeed
from feeds.pipeline_feed import PipelineFeed
from feeds.renko_feed import RenkoFeed
from utils.redis_queue import RedisQueue


class FeedHelper:
    def __init__(self, **configs):
        self.feed: BaseFeed = self.get_feed(configs)

    def next(self) -> FeedData:
        return self.feed.next(None)

    def get_feed(self, config):
        if "feed_type" in config:
            feed_type = config.pop("feed_type")
        else:
            raise Exception("Feed Type not provided")

        if feed_type == "OHLC_QUEUE_FEED":
            queue_key = config.pop("redis_feed_key")
            queue = RedisQueue(queue_key)
            return OHLCQueueFeed(queue=queue, **config)
        elif feed_type == "RENKO_FEED":
            brick_size = config.pop("brick_size")
            brick_sizer_func = config.pop("brick_sizer_func")
            return RenkoFeed(
                brick_size=brick_size, brick_sizer=brick_sizer_func, **config
            )
        elif feed_type == "PIPELINE_FEED":
            return PipelineFeed(**config.pop("pipeline_feed_config"))
        elif feed_type == "AGGREGATOR_FEED":
            return AggregatorFeed(**config.pop("aggregator_feed_config"))
        elif feed_type == "RESAMPLE_FEED":
            return ResampleFeed(**config)

        raise Exception("Invalid feed type")
