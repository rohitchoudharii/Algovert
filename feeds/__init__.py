from models import FeedData


class BaseFeed:
    def __init__(self, **configs):
        print(configs)
        self.name = configs["feed_name"]

    def next(self, data: FeedData) -> FeedData:
        raise NotImplementedError
