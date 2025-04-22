from feeds.instrument_feed import InstrumentFeed
from util.data_loader import get_historical_data
from threading import Thread
from utils.redis_queue import RedisQueue
from dotenv import load_dotenv
import pandas as pd
import backtrader as bt
import time
from strategies.supertrend_strategy import SuperTrendStrategy
from manager.strategy_manager import StrategyManager
from math import log
from broker.fyers_broker import FyersBroker
import os

load_dotenv(override=True)


initial_value = 500000
# Create Cerebro engine
cerebro = bt.Cerebro()


def brick_sizer(close, bricks):
    # if len(bricks) > 1:
    #     high = max([brick.high for brick in bricks])
    #     low = min([brick.low for brick in bricks])
    #     range = high - low
    #     # print(f"log: {log(range)} range: {range}")
    #     return range * 0.05
    # else:
    return close * 0.001


start_time = "2025-04-21 09:00"
end_time = "2025-04-21 16:00"
bot_manager = {
    "feed_details": {
        "name": "NSE:CIPLA-EQ",
        "session_time": {"start_time": start_time, "end_time": end_time},
        "feed_type": "PIPELINE_FEED",
        "pipeline_feed_config": {
            "feed_name": "renko_pipeline",
            "sub_feed_configs": [
                {
                    "feed_name": "TATASTEEL",
                    "feed_type": "OHLC_QUEUE_FEED",
                    "redis_feed_key": "NSE:CIPLA-EQ",
                },
                # {
                #     "feed_name": "renko data",
                #     "feed_type": "RENKO_FEED",
                #     "brick_size": 0,
                #     "brick_sizer_func": brick_sizer,
                # },
                {
                    "feed_name": "TATASTEEL",
                    "feed_type": "RESAMPLE_FEED",
                    "time_frame_in_seconds": 30,
                    "completed_bars_only": True,
                },
            ],
        },
    },
    "strayegy_details": {
        "entry_time": {"start_time": start_time, "end_time": end_time},
        "strategy_stop_time": end_time,
    },
}


def thread_relead_data(ticker_df: pd.DataFrame, symbol: str):
    data_queue = RedisQueue(name=symbol)
    data_queue.remove_all()
    print("Removed all previous data", symbol)

    for idx in range(ticker_df.shape[0]):
        # time.sleep(1)

        d = {}
        d["epoch"] = ticker_df.iloc[idx, 5].timestamp()
        d["ltp"] = float(ticker_df.iloc[idx, 3])
        # print(d)

        data_queue.push(d)
    print("Data loading completed: ", symbol)


for symbol in ["NSE:CIPLA-EQ"]:
    df = get_historical_data(
        symbol, "5S", "2025-04-21 09:00", "2025-04-21 16:00", False
    )

    thread = Thread(target=thread_relead_data, args=(df, symbol))
    thread.start()
time.sleep(5)

feed = InstrumentFeed(
    **bot_manager["feed_details"], timeframe=bt.TimeFrame.Seconds, compression=1
)
cerebro.adddata(feed)

cerebro.addstrategy(
    strategy=SuperTrendStrategy,
    multiplier=2,
    strategy_manager=StrategyManager(**bot_manager["strayegy_details"]),
)


cerebro.addsizer(sizercls=bt.sizers.FixedSize, stake=1)
# cerebro.broker.set_cash(initial_value)

# fyers_broker = FyersBroker(
#     client_id=os.environ["CLIENT_ID"],
#     access_token=os.environ["ACCESS_TOKEN"],
#     paper_trading=False,  # Set to True for paper trading
# )

# cerebro.setbroker(fyers_broker)
# cerebro.addobserver(bt.observers.DataTrades)

print("Strategy Started")
cerebro.run(live=True, stdstats=True)

print("Strategy completed")

cerebro.plot(style="candlestick")
