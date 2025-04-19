import backtrader as bt
from feeds.feed_helper import FeedHelper
from models import FeedData
from datetime import datetime


class InstrumentFeed(bt.DataBase):
    def __init__(self, **configs):
        print(configs)
        self.feed_helper = FeedHelper(**configs)
        self.last_update = datetime.now()

    def _load(self):
        if (datetime.now().timestamp() - self.last_update.timestamp()) > 10:
            print("Time Out")
            return False

        try:
            data_feed_dto: FeedData = self.feed_helper.next()

            if data_feed_dto is None:
                return None
            print("Date: ", data_feed_dto.datetime)

            self.lines.datetime[0] = bt.date2num(data_feed_dto.datetime)
            self.lines.open[0] = self.__round(data_feed_dto.open)
            self.lines.high[0] = self.__round(data_feed_dto.high)
            self.lines.low[0] = self.__round(data_feed_dto.low)
            self.lines.close[0] = self.__round(data_feed_dto.close)
            self.lines.volume[0] = self.__round(data_feed_dto.volume)
            self.lines.openinterest[0] = 0
            self.last_update = datetime.now()
            # print("Last updated time: ", self.last_update)

            return True

        except Exception as e:
            print("Error occured in data feed: ", e, e.with_traceback())
            return False

    def __round(self, num, decimal_digit=2):
        return round(float(num), decimal_digit)
