from feeds import BaseFeed, FeedData
from utils.renko_util import Renko


class RenkoFeed(BaseFeed):
    def __init__(self, brick_size=None, brick_sizer=None, **configs):
        super().__init__(**configs)
        self.renko = Renko(
            brick_size=brick_size, brick_calc=brick_sizer, multi_brick=False
        )

    def next(self, data: FeedData) -> FeedData:
        is_added = self.renko.create_new_brick(
            close=data.close, time_stamp=data.datetime
        )
        if is_added:
            last_brick = self.renko.bricks[-1]

            return FeedData(
                data.datetime,
                last_brick.open,
                last_brick.high,
                last_brick.low,
                last_brick.close,
                0,
                self.name,
            )
