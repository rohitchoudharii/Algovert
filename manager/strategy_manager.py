from backtrader import Strategy
from dateutil.parser import parse
import backtrader as bt


class StrategyManager:
    def __init__(self, **config):
        self.position_type = config.get("position_type", "LONG")
        print("Position type: ", self.position_type)
        self.order_details_by_data_name = config.get(
            "order_details_by_data_name", dict()
        )
        self.strategy_stop_time = config.get("strategy_stop_time")
        self.long_positions = {}
        self.short_positions = {}

    def set_strategy(self, strategy: Strategy):
        self.strategy = strategy

    def long(self, data):
        name = data._name
        if self.long_positions.get(name, False):
            # print("Already long in the market: ", name)
            return

        if name not in self.order_details_by_data_name:
            self.order_details_by_data_name[name] = {
                "order_type": "SINGLE",
                "ticker": name,
            }

        if "LONG" in self.position_type or self.short_positions.get(name, False):
            close_position = False
            if "SELL" in self.position_type:
                close_position = True

            size = self.strategy.getsizing()
            if "LONG-SHORT" in self.position_type and self.short_positions.get(
                name, False
            ):
                size *= 2

            self.strategy.buy(
                data=data,
                order_details=self.order_details_by_data_name[name],
                size=size,
                close_position=close_position,
            )
            self.short_positions[name] = False
            self.long_positions[name] = True

    def short(self, data):
        name = data._name

        if self.short_positions.get(name, False):
            # print("Already short in the market: ", name)
            return

        if name not in self.order_details_by_data_name:
            self.order_details_by_data_name[name] = {
                "order_type": "SINGLE",
                "ticker": name,
            }
        if "SHORT" in self.position_type or self.long_positions.get(name, False):
            close_position = False
            if "LONG" == self.position_type:
                close_position = True

            size = self.strategy.getsizing(data=data)
            size = self.strategy.getsizing()
            if "LONG-SHORT" in self.position_type and self.long_positions.get(
                name, False
            ):
                size *= 2

            self.strategy.sell(
                data=data,
                size=size,
                order_details=self.order_details_by_data_name[name],
                close_position=close_position,
            )
            self.long_positions[name] = False
            self.short_positions[name] = True

    def check_strategy_stop_time(self, dt):
        if self.strategy_stop_time:
            return parse(self.strategy_stop_time) < bt.num2date(dt)
        return False

    def _check_session_range(self, dt):
        if self.session_time:
            start_time_condition = True
            end_time_condition = True

            if self.session_time.get("start_time"):
                start_time_condition = parse(self.session_time.get("start_time")) <= dt
            if self.session_time.get("end_time"):
                end_time_condition = parse(self.session_time.get("end_time")) >= dt
            return start_time_condition and end_time_condition
        else:
            return False
