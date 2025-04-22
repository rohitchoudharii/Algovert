# flake8: noqa
import math
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.widgets import Cursor
import numpy as np
from enum import Enum
import pandas as pd
from typing import List, Callable
import json
from datetime import datetime
import talib
import pandas_ta as ta
import plotly.graph_objects as go
from tqdm import tqdm

# matplotlib.use("TkAgg")


class BrickType(Enum):
    UP = "UP"
    DOWN = "DOWN"
    FIRST = "FIRST"


class Brick:
    def __init__(
        self,
        type: BrickType,
        start_time: datetime,
        end_time: datetime,
        open: float,
        offset_open: float,
        close: float,
    ):
        self.brick_type = type
        self.start_time = start_time
        self.end_time = end_time
        self.open = open
        self.close = close
        self.offset_open = offset_open
        self.brick_size = abs(close - open)

        if self.brick_type == BrickType.UP:
            self.high = self.close
            self.low = self.open
        elif self.brick_type == BrickType.DOWN:
            self.high = self.open
            self.low = self.close
        else:
            self.high = self.open
            self.low = self.close

    def is_close_between_brick(self, incoming_close):
        return (self.open > incoming_close and self.close < incoming_close) or (
            self.open < incoming_close and self.close > incoming_close
        )

    def to_dict(self):
        return {
            "type": self.brick_type.value,
            "start_time": str(self.start_time),
            "end_time": str(self.end_time),
            "open": self.open,
            "offset_open": self.offset_open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "size": self.brick_size,
        }

    def __repr__(self):
        return json.dumps(self.to_dict())


class Renko:
    """Renko initialization class"""

    def __init__(
        self,
        data: pd.DataFrame = None,
        brick_size: float = None,
        multi_brick: bool = True,
        brick_calc: Callable[[float], float] = None,
        sticky_close: bool = True,
    ):
        if data is not None:
            self.data = data
            self.start_time = data.iloc[0]["timestamp"]
            self.end_time = data.iloc[0]["timestamp"]

        self.multi_brick = multi_brick
        self.sticky_close = sticky_close
        self.bricks: List[Brick] = []

        if not brick_calc and not brick_size:
            raise RuntimeError("Please provide brick details")
        self.brick_size = brick_size
        self.brick_calc = brick_calc

    def create_renko(self):
        """Creating renko bricks using the provided close data"""
        for index in tqdm(range(len(self.data)), ncols=100):
            close = float(self.data.iloc[index]["close"])

            if index == 0:
                if len(self.bricks) == 0:
                    brick_size = self.get_brick_size(close)
                    first_brick = float(close // brick_size * brick_size)
                    # print(first_brick)
                    self.append_brick(
                        BrickType.FIRST,
                        float(first_brick),
                        float(first_brick),
                        float(first_brick),
                        self.start_time,
                        self.end_time,
                    )
            else:
                self.create_new_brick(close, self.data.iloc[index]["timestamp"])

    def get_brick_size(self, close):
        if self.brick_calc != None:
            return self.brick_calc(close, self.bricks)
        return self.brick_size

    def create_new_brick(self, close, time_stamp):
        if len(self.bricks) == 0:
            self.start_time = time_stamp
            self.end_time = time_stamp
            brick_size = self.get_brick_size(close)
            first_brick = float(close // brick_size * brick_size)
            # print(first_brick)
            self.append_brick(
                BrickType.FIRST,
                float(first_brick),
                float(first_brick),
                float(first_brick),
                self.start_time,
                self.end_time,
            )
            return True

        prev_brick = self.bricks[-1]
        brick_size = self.get_brick_size(prev_brick.close)

        # Calculate brick start_time
        if prev_brick.is_close_between_brick(close):
            self.start_time = time_stamp
        self.end_time = time_stamp

        delta = 0
        type = BrickType.UP
        # Calculate brick construction
        if prev_brick.brick_type == BrickType.UP:
            if close > prev_brick.close:
                delta = close - prev_brick.close
                type = BrickType.UP
            elif close < prev_brick.offset_open:
                delta = prev_brick.offset_open - close
                type = BrickType.DOWN
        elif prev_brick.brick_type == BrickType.DOWN:
            if close < prev_brick.close:
                delta = prev_brick.close - close
                type = BrickType.DOWN
            elif close > prev_brick.offset_open:
                delta = close - prev_brick.offset_open
                type = BrickType.UP
        else:
            if close > prev_brick.close:
                delta = close - prev_brick.close
                type = BrickType.UP
            if close < prev_brick.close:
                delta = prev_brick.close - close
                type = BrickType.DOWN
        total_bricks = math.floor(delta / brick_size)
        if total_bricks != 0:
            self.add_bricks(
                type, total_bricks, brick_size, self.start_time, self.end_time
            )
            self.start_time = time_stamp
        return total_bricks != 0

    def add_bricks(
        self,
        type: BrickType,
        count: int,
        brick_size: int,
        start_time: datetime,
        end_time: datetime,
    ):
        if self.multi_brick:
            for i in range(count):
                self.__add_bricks(type, 1, brick_size, start_time, end_time)
        else:
            self.__add_bricks(type, count, brick_size, start_time, end_time)

    def __add_bricks(
        self,
        type: BrickType,
        count: int,
        brick_size: int,
        start_time: datetime,
        end_time: datetime,
    ):
        """Adds brick(s) to the bricks list
        :param type: type of brick (up or down)
        :type type: string
        :param count: number of bricks to add
        :type count: int
        :param brick_size: brick size
        :type brick_size: float
        """
        prev_brick = self.bricks[-1]
        open = 0
        close = 0
        offset_open = 0

        if type == BrickType.UP:
            if (
                prev_brick.brick_type == BrickType.UP
                or prev_brick.brick_type == BrickType.FIRST
            ):
                offset_open = prev_brick.close + brick_size * (count - 1)
                close = prev_brick.close + brick_size * count
                open = prev_brick.close
            elif prev_brick.brick_type == BrickType.DOWN:
                offset_open = prev_brick.offset_open + brick_size * (count - 1)
                close = prev_brick.offset_open + brick_size * count
                open = prev_brick.offset_open
        elif type == BrickType.DOWN:
            if prev_brick.brick_type == BrickType.UP:
                offset_open = prev_brick.offset_open - brick_size * (count - 1)
                close = prev_brick.offset_open - brick_size * count
                open = prev_brick.offset_open
            elif (
                prev_brick.brick_type == BrickType.DOWN
                or prev_brick.brick_type == BrickType.FIRST
            ):
                offset_open = prev_brick.close - brick_size * (count - 1)
                close = prev_brick.close - brick_size * count
                open = prev_brick.close

        self.append_brick(type, open, offset_open, close, start_time, end_time)

    def append_brick(
        self,
        type: BrickType,
        open: float,
        offset_open: float,
        close: float,
        start_time: datetime,
        end_time: datetime,
    ):
        self.bricks.append(Brick(type, start_time, end_time, open, offset_open, close))

    def add_single_custom_brick(
        self,
        type: str,
        open: float,
        close: float,
        start_time: datetime,
        end_time: datetime,
    ):
        """Mainly used for adding the first brick in live strategies.
        :param type: type of brick, up or down
        :type type: string
        :param open: open close of the brick
        :type open: float
        :param close: close close of the brick
        :type close: float
        """

        self.append_brick(BrickType(type), open, open, close, start_time, end_time)

    def get_dataframe(self):
        bricks_dicts = [b.to_dict() for b in self.bricks]
        df = pd.DataFrame(bricks_dicts)
        return df

    def draw_chart(self, x_slice=10, **plt_krgs):
        bricks_dicts = [b.to_dict() for b in self.bricks]

        df = pd.DataFrame(bricks_dicts)
        df["start_time"] = pd.to_datetime(df["start_time"])

        fig, ax = plt.subplots(**plt_krgs)
        plt.plot(df.index, df["close"], alpha=0)
        df["psar"] = talib.SAR(df["high"], df["low"], 0.02, 0.2)

        st = df.ta.supertrend(length=2, multiplier=2)

        plt.plot(df.index, st[st.columns[-1]], color="red")

        plt.plot(df.index, st[st.columns[-2]], color="green")

        plt.plot(df.index, df["psar"], color="blue")

        for i, row in df.iterrows():
            color = "green" if row["close"] > row["open"] else "red"
            lower = min(row["open"], row["close"])
            height = abs(row["close"] - row["open"])

            # Draw the rectangle (Renko box)
            rect = Rectangle(
                (i, lower), 1, height, linewidth=1, edgecolor="black", facecolor=color
            )
            ax.add_patch(rect)

        cursor = Cursor(ax, useblit=True, color="red", linewidth=1)

        # Add timestamps as x-ticks (every 10th brick for clarity)
        jump_size = round(len(df) / x_slice)
        plt.xticks(
            df.index[::jump_size],
            df[::jump_size]["start_time"].dt.strftime("%d %H:%M"),
            rotation=45,
        )

        plt.xlabel("Brick (Sequential)")
        plt.ylabel("Renko Close")
        plt.show()
        fig.show()

    def draw_interactive_renko(
        self,
        start_range=0,
        end_range=1000,
        super_trend=(2, 2),
        x_slice=10,
        timestamp_column="end_time",
        x_axis_format="%y %m %d %H:%M:%S",
        chart_title="",
        **plt_krgs,
    ):
        bricks_dicts = [b.to_dict() for b in self.bricks]

        df = pd.DataFrame(bricks_dicts)

        df[timestamp_column] = pd.to_datetime(df[timestamp_column])

        df["psar"] = talib.SAR(df["high"], df["low"], 0.02, 0.2)

        st = df.ta.supertrend(length=super_trend[0], multiplier=super_trend[1])
        df = df[start_range:end_range]
        st = st[start_range:end_range]

        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=df.index,
                    open=df["open"],
                    high=df["high"],
                    low=df["low"],
                    close=df["close"],
                )
            ]
        )

        fig.add_trace(go.Scatter(x=df.index, y=st[st.columns[-1]], mode="lines"))

        fig.add_trace(go.Scatter(x=df.index, y=st[st.columns[-2]], mode="lines"))

        # fig.add_trace(go.Line(x=df.index, y=df["psar"]))

        fig.update_layout(
            title=chart_title,
            plot_bgcolor="white",  # Transparent plot area
            # paper_bgcolor="rgba(0,0,0,0)",  # Transparent outer area
            xaxis=dict(
                tickvals=df.index,
                ticktext=df[timestamp_column].dt.strftime(x_axis_format),
                showspikes=True,  # Enable x-axis spikes
                spikemode="toaxis+across+marker",  # Full crosshair + marker
                spikethickness=0.5,  # Line thickness
                spikecolor="black",  # Crosshair color
                spikesnap="cursor",
            ),
            autosize=True,
            margin=dict(l=1, r=1, t=50, b=1),
            yaxis=dict(
                showspikes=True,  # Enable y-axis spikes
                spikemode="toaxis+across+marker",  # Full crosshair only
                spikethickness=0.5,
                spikecolor="black",
                spikesnap="cursor",
            ),
        )

        fig.show()
