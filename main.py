from feeds.instrument_feed import InstrumentFeed
from util.data_loader import get_historical_data
from threading import Thread
from utils.redis_queue import RedisQueue
from dotenv import load_dotenv
import pandas as pd
import backtrader as bt
import time

load_dotenv()


initial_value = 5000000
# Create Cerebro engine
cerebro = bt.Cerebro()


config = {
    "feed_type": "PIPELINE_FEED",
    "pipeline_feed_config": {
        "feed_name": "renko_pipeline",
        "sub_feed_configs": [
            {
                "feed_name": "aggregate",
                "feed_type": "AGGREGATOR_FEED",
                "aggregator_feed_config": {
                    "feed_name": "aggregate",
                    "sub_feed_configs": [
                        {
                            "feed_name": "NIFTY25APR23300CE",
                            "feed_type": "OHLC_QUEUE_FEED",
                            "redis_feed_key": "NSE:NIFTY25APR23300CE",
                            "operator": "ADD",
                            "multiplier": 2,
                        },
                        {
                            "feed_name": "NIFTY2543023300PE",
                            "feed_type": "OHLC_QUEUE_FEED",
                            "redis_feed_key": "NSE:NIFTY2543023300PE",
                            "operator": "SUBSTRACT",
                        },
                        {
                            "feed_name": "NIFTY2543023600CE",
                            "feed_type": "OHLC_QUEUE_FEED",
                            "redis_feed_key": "NSE:NIFTY2543023600CE",
                            "operator": "SUBSTRACT",
                        },
                        {
                            "feed_name": "NIFTY25APR23600PE",
                            "feed_type": "OHLC_QUEUE_FEED",
                            "redis_feed_key": "NSE:NIFTY25APR23600PE",
                            "operator": "ADD",
                            "multiplier": 2,
                        },
                    ],
                },
            },
            {
                "feed_name": "renko data",
                "feed_type": "RENKO_FEED",
                "brick_size": 0,
                "brick_sizer_func": lambda x: x * 0.02,
            },
        ],
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


for symbol in [
    "NSE:NIFTY25APR23300CE",
    "NSE:NIFTY2543023300PE",
    "NSE:NIFTY2543023600CE",
    "NSE:NIFTY25APR23600PE",
]:
    df = get_historical_data(
        symbol, "5S", "2025-04-16 09:00", "2025-04-16 16:00", False
    )

    thread = Thread(target=thread_relead_data, args=(df, symbol))
    thread.start()
time.sleep(5)

feed = InstrumentFeed(**config)
cerebro.adddata(feed, name=symbol + "_renko")


print("Strategy Started")
cerebro.run(live=True)

print("Strategy completed")

cerebro.plot(style="candlestick")
