from feeds import BaseFeed, FeedData
from utils.resampler_util import DataResampler
from datetime import datetime


class ResampleFeed(BaseFeed):
    def __init__(self, **configs):
        super().__init__(**configs)
        self.time_frame_in_seconds = configs.get("time_frame_in_seconds", 5)

        self.sampler = DataResampler(timeframe_seconds=self.time_frame_in_seconds)
        self.prev_bar = None
        self.completed_bars_only = configs.get("completed_bars_only", True)

    def next(self, data: FeedData) -> FeedData:
        self.sampler.update(data)

        if self.completed_bars_only:
            bar = self.sampler.get_prev_bar()
        else:
            bar = self.sampler.get_current_bar()

        # print("Calculated Bar: ", bar)
        if not bar:
            # print("No bar found")
            return None

        if not self.prev_bar or (self.prev_bar.get("timestamp") < bar.get("timestamp")):
            self.prev_bar = bar
            return FeedData(
                bar["timestamp"],
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                0,
                self.name,
            )
