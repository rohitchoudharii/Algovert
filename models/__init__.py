from datetime import datetime
from dataclasses import dataclass


@dataclass
class FeedData:
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    symbol: str

    # Arithmetic operators
    def __add__(self, other):
        if isinstance(other, FeedData):
            return FeedData(
                self.datetime,
                self.open + other.open,
                self.high + other.high,
                self.low + other.low,
                self.close + other.close,
                self.volume + other.volume,
                self.symbol,
            )
        return NotImplemented

    def __sub__(self, other):
        if isinstance(other, FeedData):
            return FeedData(
                self.datetime,
                self.open - other.open,
                self.high - other.high,
                self.low - other.low,
                self.close - other.close,
                self.volume - other.volume,
                self.symbol,
            )
        return NotImplemented

    def __mul__(self, scalar):
        if isinstance(scalar, (int, float)):
            return FeedData(
                self.datetime,
                self.open * scalar,
                self.high * scalar,
                self.low * scalar,
                self.close * scalar,
                self.volume * scalar,
                self.symbol,
            )
        return NotImplemented

    def __truediv__(self, scalar):
        if isinstance(scalar, (int, float)):
            return FeedData(
                self.datetime,
                self.open / scalar,
                self.high / scalar,
                self.low / scalar,
                self.close / scalar,
                self.volume / scalar,
                self.symbol,
            )
        return NotImplemented

    # Reverse arithmetic operators
    def __radd__(self, other):
        return self.__add__(other)

    def __rsub__(self, other):
        if isinstance(other, FeedData):
            return other.__sub__(self)
        return NotImplemented

    def __rmul__(self, scalar):
        return self.__mul__(scalar)

    def __rtruediv__(self, scalar):
        if isinstance(scalar, (int, float)):
            return FeedData(
                self.datetime,
                scalar / self.open,
                scalar / self.high,
                scalar / self.low,
                scalar / self.close,
                scalar / self.volume,
                self.symbol,
            )
        return NotImplemented

    def __abs__(self):
        return FeedData(
            self.datetime,
            abs(self.open),
            abs(self.high),
            abs(self.low),
            abs(self.close),
            abs(self.volume),
            self.symbol,
        )

    # Comparison operators (by close price)
    def _as_tuple(self):
        # You can choose which fields to include in the comparison
        return (
            self.datetime,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.symbol,
        )

    def __eq__(self, other):
        if isinstance(other, FeedData):
            return self._as_tuple() == other._as_tuple()
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, FeedData):
            return self._as_tuple() != other._as_tuple()
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, FeedData):
            return self._as_tuple() < other._as_tuple()
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, FeedData):
            return self._as_tuple() <= other._as_tuple()
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, FeedData):
            return self._as_tuple() > other._as_tuple()
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, FeedData):
            return self._as_tuple() >= other._as_tuple()
        return NotImplemented

    # String representation
    def __str__(self):
        return (
            f"{self.symbol} {self.datetime} O:{self.open} H:{self.high} "
            f"L:{self.low} C:{self.close} V:{self.volume}"
        )

    def __repr__(self):
        return (
            f"FeedData(datetime={self.datetime!r}, open={self.open}, "
            f"high={self.high}, low={self.low}, close={self.close}, "
            f"volume={self.volume}, symbol={self.symbol!r})"
        )
