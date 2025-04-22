from __future__ import absolute_import, division, print_function, unicode_literals

import collections
import datetime
from typing import List

from backtrader.broker import BrokerBase
from backtrader.order import Order, BuyOrder, SellOrder
from backtrader.position import Position
from backtrader.metabase import MetaParams

from fyers_apiv3 import fyersModel


# class MetaFyersBroker(MetaBroker, MetaParams):
#     def __init__(cls, name, bases, dct):
#         super(MetaFyersBroker, cls).__init__(name, bases, dct)


class FyersBroker(BrokerBase, metaclass=MetaParams):
    params = (
        ("client_id", None),
        ("access_token", None),
        ("paper_trading", False),  # Paper trading flag
        ("commission", 0.0),
        ("margin", None),  # Cash to use for margin operations
        ("id", ""),
    )

    # Order statuses for Fyers
    FYERS_ORDER_STATUSES = {
        1: Order.Submitted,  # Pending
        2: Order.Accepted,  # Traded/Filled
        3: Order.Rejected,  # Rejected
        4: Order.Cancelled,  # Cancelled
        5: Order.Partial,  # Partially Filled
    }

    # Order types mapping
    FYERS_ORDER_TYPES = {
        Order.Market: 2,  # Market order
        Order.Limit: 1,  # Limit order
        Order.Stop: 3,  # SL order
        Order.StopLimit: 4,  # SL-M order
    }

    # Order sides mapping
    FYERS_ORDER_SIDES = {
        Order.Buy: 1,  # Buy
        Order.Sell: -1,  # Sell
    }

    def __init__(self):
        super(FyersBroker, self).__init__()
        self.orders = collections.OrderedDict()  # orders by order id
        self.notifs = collections.deque()  # holds notifications for the broker
        self.positions = collections.defaultdict(Position)  # holds positions

        # Initialize Fyers API client
        if not self.p.paper_trading and self.p.client_id and self.p.access_token:
            self.fyers_client = fyersModel.FyersModel(
                client_id=self.p.client_id, token=self.p.access_token, is_async=False
            )
        else:
            self.fyers_client = None

        self.startingcash = self.getcash()

    def rate_limit(seconds):
        def limiter_decorator(func):
            last_run = {}
            res_dict = {}

            def limiter(*args, **kargs):
                if (
                    last_run.get(func.__name__, 0) + seconds
                    < datetime.datetime.now().timestamp()
                ):
                    # print(datetime.datetime.now(), "Running the api", func.__name__)
                    res_dict[func.__name__] = func(*args, **kargs)
                    last_run[func.__name__] = datetime.datetime.now().timestamp()
                    return res_dict.get(func.__name__)
                else:
                    # print("Rate limitted", func.__name__)
                    return res_dict.get(func.__name__)

            return limiter

        return limiter_decorator

    def start(self):
        super(FyersBroker, self).start()
        if (
            not self.p.paper_trading
            and self.fyers_client is None
            and self.p.client_id
            and self.p.access_token
        ):
            self.fyers_client = fyersModel.FyersModel(
                client_id=self.p.client_id, token=self.p.access_token, is_async=False
            )

    def stop(self):
        super(FyersBroker, self).stop()

    @rate_limit(10)
    def getcash(self):
        # print("Cash called")
        # If paper trading, return super implementation
        if self.p.paper_trading:
            return super(FyersBroker, self).getcash()

        # Get funds from Fyers API
        if self.fyers_client:
            try:
                response = self.fyers_client.funds()
                print("Response: ", response)
                if response["s"] == "ok":
                    for fund in response["fund_limit"]:
                        if fund["id"] == 1:
                            print("Balance: ", fund["equityAmount"])
                            self.cash_available = fund["equityAmount"]
                            return self.cash_available
            except Exception as e:
                print(f"Error getting cash: {e}")
                return self.cash_available or 0

        return super(FyersBroker, self).getcash()

    @rate_limit(10)
    def getvalue(self):
        print("Broker value")
        # If paper trading, return super implementation
        if self.p.paper_trading:
            return super(FyersBroker, self).getvalue()

        # Calculate portfolio value from Fyers API
        value = self.getcash()

        if self.fyers_client:
            try:
                # Get positions value
                positions_response = self.fyers_client.positions()
                if positions_response["s"] == "ok":
                    for position in positions_response["netPositions"]:
                        value += position["realized_profit"]
                        value += position["unrealized_profit"]
            except Exception as e:
                print(f"Error getting positions value: {e}")

        return value

    @rate_limit(10)
    def getposition(self, data):
        # print("Get position")
        # If paper trading, return super implementation
        if self.p.paper_trading:
            return self.positions[data._name]

        # Get position from Fyers API
        if self.fyers_client:
            try:
                symbol = data._name
                positions_response = self.fyers_client.positions()
                if positions_response["s"] == "ok":
                    for position in positions_response["netPositions"]:
                        if position["symbol"] == symbol:
                            size = position["qty"]
                            price = position["netAvg"]
                            return Position(size, price)
            except Exception as e:
                print(f"Error getting position: {e}")

        return self.positions[data._name]

    def _submit(self, order):
        order.submit(self)
        self.notifs.append(order.clone())
        return order

    def _reject(self, order):
        order.reject(self)
        self.notifs.append(order.clone())
        return order

    def _accept(self, order):
        order.accept()
        self.notifs.append(order.clone())
        return order

    def _cancel(self, order):
        order.cancel()
        self.notifs.append(order.clone())
        return order

    def _fill(self, order: Order, price, size, **kwargs):
        # order.execute(dt=datetime.datetime.now(), price=price, size=size, **kwargs)
        order.completed()
        pos = self.positions[order.data._name]
        pos.update(order.size, price)

        self.notifs.append(order.clone())
        return order

    def _get_side(self, order, position_side=None, close_position=False):
        sides = {"BUY": 1, "SELL": -1}
        if position_side:
            side = sides.get(position_side, 1)
        else:
            side = self.FYERS_ORDER_SIDES.get(order.ordtype, 1)  # Default to Buy
            close_position = False

        if close_position:
            side = side * -1
        return side

    def _create_order_params_from_order(
        self, order, position_side=None, ticker=None, multiplier=1, close_position=False
    ):
        """
        Helper method to create order parameters from an order object

        Args:
            order: The backtrader order object
            ticker: Optional override for the symbol
            multiplier: Quantity multiplier (default: 1)

        Returns:
            Dictionary of order parameters for Fyers API
        """
        symbol = ticker if ticker else order.data._name
        qty = int(abs(order.size) * multiplier)
        side = self._get_side(order, position_side, close_position)
        order_type = self.FYERS_ORDER_TYPES.get(order.exectype, 2)  # Default to Market

        return {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": order_type,
            "productType": "CNC",  # Default to CNC, can be changed if needed
            "limitPrice": order.price
            if order.exectype in [Order.Limit, Order.StopLimit]
            else 0,
            "stopPrice": order.pricelimit
            if order.exectype in [Order.Stop, Order.StopLimit]
            else 0,
            "validity": "DAY",  # Default to DAY validity
            "disclosedQty": 0,
            "offlineOrder": False,
        }

    def buy(
        self,
        owner,
        data,
        size,
        price=None,
        plimit=None,
        exectype=None,
        valid=None,
        tradeid=0,
        oco=None,
        trailamount=None,
        trailpercent=None,
        order_details=None,
        close_position=None,
        **kwargs,
    ):
        order = BuyOrder(
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            trailamount=trailamount,
            trailpercent=trailpercent,
            **kwargs,
        )

        # Always store order details
        order.order_details = order_details
        order.close_position = close_position

        order = self._submit(order)
        self.orders[order.ref] = order
        return self._process_order(order)

    def sell(
        self,
        owner,
        data,
        size,
        price=None,
        plimit=None,
        exectype=None,
        valid=None,
        tradeid=0,
        oco=None,
        trailamount=None,
        trailpercent=None,
        order_details=None,
        close_position=None,
        **kwargs,
    ):
        order = SellOrder(
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            trailamount=trailamount,
            trailpercent=trailpercent,
            **kwargs,
        )

        # Always store order details
        order.order_details = order_details
        order.close_position = close_position

        order = self._submit(order)
        self.orders[order.ref] = order
        return self._process_order(order)

    def _process_order(self, order):
        # If paper trading, don't process through API
        if self.p.paper_trading:
            return order

        if not self.fyers_client:
            self._reject(order)
            return order

        try:
            close_position = order.close_position
            order_details = order.order_details
            order_type = order_details.get("order_type", "SINGLE")

            if order_type == "SINGLE":
                # Single order
                ticker = order_details.get("ticker", order.data._name)
                multiplier = order_details.get("multiplier", 1)
                position_side = order_details.get("position_side", None)

                # Create order parameters
                order_params = self._create_order_params_from_order(
                    order,
                    position_side=position_side,
                    ticker=ticker,
                    multiplier=multiplier,
                    close_position=close_position,
                )
                print(datetime.datetime.now(), "Triggered order api")
                response = self.fyers_client.place_order(order_params)

                if response["s"] == "ok":
                    order.fyers_order_id = response["id"]
                    self._accept(order)
                else:
                    self._reject(order)
                    print(f"Order rejected: {response['message']}")

            elif order_type == "BUCKET":
                # Handle bucket order (multiple orders)
                bucket_orders = order_details.get("bucketOrders", [])
                if not bucket_orders:
                    self._reject(order)
                    print("Bucket order with no orders specified")
                    return order

                # Prepare orders list for batch submission
                orders_params = []
                for bucket_order in bucket_orders:
                    ticker = bucket_order.get("ticker")
                    if not ticker:
                        continue

                    multiplier = bucket_order.get("multiplier", 1)
                    position_side = bucket_order.get("position_side", None)

                    # Create order parameters
                    order_params = self._create_order_params_from_order(
                        order,
                        ticker=ticker,
                        position_side=position_side,
                        multiplier=multiplier,
                        close_position=close_position,
                    )
                    orders_params.append(order_params)

                if not orders_params:
                    self._reject(order)
                    print("No valid orders in bucket")
                    return order

                # Submit batch order
                response = self.fyers_client.place_basket_orders(data=orders_params)

                if response["s"] == "ok":
                    # Store all order IDs in a list
                    order.fyers_order_ids = []
                    all_successful = True

                    for i, order_response in enumerate(response["data"]):
                        if order_response["s"] == "ok":
                            order.fyers_order_ids.append(order_response["id"])
                        else:
                            all_successful = False
                            print(
                                f"Sub-order {i} rejected: {order_response['message']}"
                            )

                    if all_successful:
                        self._accept(order)
                    else:
                        # Mark as partial if at least one order succeeded
                        if order.fyers_order_ids:
                            order.accept()
                            order.partial()
                            self.notifs.append(order.clone())
                        else:
                            self._reject(order)
                else:
                    self._reject(order)
                    print(f"Bucket order rejected: {response['message']}")
            else:
                self._reject(order)
                print(f"Unknown order type: {order_type}")

        except Exception as e:
            print(f"Error processing order: {e}")
            self._reject(order)

        return order

    def cancel(self, order):
        if not self.fyers_client or self.p.paper_trading:
            self._cancel(order)
            return

        # Handle cancellation of bucket orders (multiple order IDs)
        if hasattr(order, "fyers_order_ids") and order.fyers_order_ids:
            cancelled_all = True
            for order_id in order.fyers_order_ids:
                try:
                    response = self.fyers_client.cancel_order({"id": order_id})
                    if response["s"] != "ok":
                        cancelled_all = False
                        print(
                            f"Failed to cancel order {order_id}: {response['message']}"
                        )
                except Exception as e:
                    cancelled_all = False
                    print(f"Error cancelling order {order_id}: {e}")

            if cancelled_all:
                self._cancel(order)
            return

        # Handle single order cancellation
        if not hasattr(order, "fyers_order_id"):
            self._reject(order)
            return

        try:
            response = self.fyers_client.cancel_order({"id": order.fyers_order_id})

            if response["s"] == "ok":
                self._cancel(order)
            else:
                print(f"Failed to cancel order: {response['message']}")
        except Exception as e:
            print(f"Error cancelling order: {e}")

    def get_order_details(self, order):
        """
        Get details of a specific order from Fyers

        Args:
            order: The order object with fyers_order_id attribute

        Returns:
            Order details dictionary from Fyers, or None if not found
        """
        print("Get _ order_ details")
        if self.p.paper_trading or not self.fyers_client:
            return None

        if not hasattr(order, "fyers_order_id"):
            return None

        try:
            response = self.fyers_client.orderbook()
            if response["s"] == "ok":
                for order_detail in response["orderBook"]:
                    if order_detail["id"] == order.fyers_order_id:
                        return order_detail
        except Exception as e:
            print(f"Error getting order details: {e}")

        return None

    def get_orders(self):
        """
        Get all orders from Fyers

        Returns:
            List of order details dictionaries, or empty list if error
        """
        # print("Get ordersss")
        if self.p.paper_trading or not self.fyers_client:
            return []

        try:
            response = self.fyers_client.orderbook()
            if response["s"] == "ok":
                return response["orderBook"]
        except Exception as e:
            print(f"Error getting orders: {e}")

        return []

    @rate_limit(10)
    def get_positions(self):
        """
        Get all positions from Fyers

        Returns:
            List of position details dictionaries, or empty list if error
        """
        print("get_positions")
        if self.p.paper_trading or not self.fyers_client:
            return []

        try:
            response = self.fyers_client.positions()
            if response["s"] == "ok":
                return response["netPositions"]
        except Exception as e:
            print(f"Error getting positions: {e}")

        return []

    def update_positions(self):
        """
        Update the broker's position dictionary with current positions from Fyers
        """
        if self.p.paper_trading or not self.fyers_client:
            return

        try:
            positions = self.get_positions()
            for position in positions:
                symbol = position["symbol"]
                size = position["qty"]
                price = position["netAvg"]
                self.positions[symbol] = Position(size, price)
        except Exception as e:
            print(f"Error updating positions: {e}")

    @rate_limit(10)
    def next(self):
        # If using real trading, update orders status from Fyers

        if not self.p.paper_trading and self.fyers_client:
            self.update_orders()
            # self.update_positions()

    def update_orders(self):
        """
        Update the status of all open orders from Fyers
        """
        if self.p.paper_trading or not self.fyers_client:
            return

        try:
            fyers_orders = self.get_orders()
            for order in [order for order in self.orders.values() if order.alive()]:
                filtered_fyers_orders = self.linked_orders(order, fyers_orders)

                all_accepted = True
                prices = []
                print("Filtered orders: ", filtered_fyers_orders)

                for fyers_order in filtered_fyers_orders:
                    status = fyers_order["status"]

                    if status == 2:  # Filled
                        prices.append(float(fyers_order["tradedPrice"]))

                    elif status == 3:  # Reject
                        all_accepted = False
                        # self._reject(order)

                    elif status == 4:  # Cancelled
                        all_accepted = False
                        # self._cancel(order)

                if all_accepted:
                    self._fill(order, sum(prices) / len(prices), 0)

        except Exception as e:
            print(f"Error updating orders: {e}")

    def linked_orders(self, order, fyers_orders: List):
        fyers_order_ids = []
        if hasattr(order, "fyers_order_id"):
            fyers_order_ids.append(order.fyers_order_id)
        elif hasattr(order, "fyers_order_ids"):
            fyers_order_ids.extend(order.fyers_order_ids)

        orders = []
        for fyers_order in fyers_orders:
            if fyers_order["id"] in fyers_order_ids:
                orders.append(fyers_order)

        return orders

    def get_notification(self):
        """
        Return the pending notifications in FIFO order
        """
        if self.notifs:
            return self.notifs.popleft()
