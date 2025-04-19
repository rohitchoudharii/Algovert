from feeds import BaseFeed, FeedData
from typing import List
from datetime import datetime


class PipelineFeed(BaseFeed):
    def __init__(self, **configs):
        from feeds.feed_helper import FeedHelper

        super().__init__(**configs)
        self.feeds: List[BaseFeed] = []
        sub_feed_configs = configs["sub_feed_configs"]
        for sub_feed_config in sub_feed_configs:
            self.feeds.append(FeedHelper(**sub_feed_config).feed)

    def next(self, data: FeedData) -> FeedData:
        derived_feed_data = data
        # print("Pipeline Data: ", data)
        # start_time = datetime.now()
        for feed in self.feeds:
            derived_feed_data = feed.next(derived_feed_data)
            if derived_feed_data is None:
                return None
        # print(
        #     "Time taken to process the pipeline: ",
        #     (datetime.now().timestamp() - start_time.timestamp()) * 1000,
        # )
        return derived_feed_data
