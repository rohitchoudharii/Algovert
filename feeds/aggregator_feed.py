from . import FeedData, BaseFeed
from typing import Dict, List


class AggregatorFeed(BaseFeed):
    def __init__(self, **configs):
        from feeds.feed_helper import FeedHelper

        super().__init__(**configs)
        self.sub_feed_configs = configs.get("sub_feed_configs")
        self.feeds: List[BaseFeed] = []
        for sub_feed_config in self.sub_feed_configs:
            self.feeds.append(FeedHelper(**sub_feed_config).feed)

        self.prev_feed_data = {feed.name: None for feed in self.feeds}

    def next(self, data: FeedData) -> FeedData:
        new_feed_datas = {}
        error_count = 0
        for feed in self.feeds:
            new_feed_data = feed.next(data)
            # print(new_feed_data)
            if new_feed_data is not None:
                new_feed_datas[feed.name] = new_feed_data
            else:
                error_count += 1
                new_feed_datas[feed.name] = self.prev_feed_data[feed.name]

        if error_count == len(new_feed_datas):
            return None

        if None in new_feed_datas.values():
            self.prev_feed_data = new_feed_datas
            return None

        derived_feed_data = self.evaluate_data(new_feed_datas)
        self.prev_feed_data = new_feed_datas

        return derived_feed_data

    def evaluate_data(self, new_datas: Dict[str, FeedData]):
        max_date = max([new_data.datetime for new_data in new_datas.values()])
        data = FeedData(max_date, 0, 0, 0, 0, 0, self.name)

        for sub_feed_config in self.sub_feed_configs:
            new_data = new_datas[sub_feed_config["feed_name"]]

            new_data *= sub_feed_config.get("multiplier", 1)

            if sub_feed_config["operator"] == "ADD":
                data += new_data
            elif sub_feed_config["operator"] == "SUBSTRACT":
                data -= new_data

        return abs(data)
