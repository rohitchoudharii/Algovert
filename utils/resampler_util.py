from datetime import datetime, timedelta
from models import FeedData


class DataResampler:
    def __init__(self, timeframe_seconds=60):
        self.timeframe = timedelta(seconds=timeframe_seconds)
        self.current_bar = None
        self.current_start = None
        self.ohlcv = []

    def _get_bar_start_time(self, timestamp):
        return timestamp - timedelta(
            seconds=timestamp.second % self.timeframe.total_seconds(),
            microseconds=timestamp.microsecond,
        )

    def update(self, feed_data: FeedData):
        dt = feed_data.datetime
        price = feed_data.close

        bar_start_time = self._get_bar_start_time(dt)
        # print(bar_start_time)

        # New bar starts
        if self.current_start != bar_start_time:
            if self.current_bar:
                self.ohlcv.append(self.current_bar)
                # print("Completed bar:", self.current_bar)

            self.current_start = bar_start_time
            self.current_bar = {
                "timestamp": bar_start_time,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
            }
        else:
            # Update current bar
            self.current_bar["high"] = max(self.current_bar["high"], price)
            self.current_bar["low"] = min(self.current_bar["low"], price)
            self.current_bar["close"] = price

    def get_current_bar(self):
        return self.current_bar

    def get_prev_bar(self):
        if len(self.ohlcv) > 0:
            return self.ohlcv[-1]
        return None


# Example usage:
if __name__ == "__main__":
    import time
    import random

    aggregator = DataResampler(timeframe_seconds=5)

    now = datetime.now()
    for i in range(120):  # simulate 2 minutes of ticks
        tick = {
            "epoch": now + timedelta(seconds=i),
            "ltp": 100 + random.uniform(-1, 1),
        }
        aggregator.update(tick)
        time.sleep(0.01)  # simulate time delay in streaming
