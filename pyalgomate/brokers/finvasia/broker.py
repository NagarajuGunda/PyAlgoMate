"""
.. moduleauthor:: Nagaraju Gunda
"""

import asyncio
import calendar
import datetime
import json
import logging
import queue
import threading
import time
from typing import ForwardRef, List, Set

import pandas as pd
import six
import zmq
from NorenRestApiAsync.NorenApiAsync import NorenApiAsync
from NorenRestApiPy.NorenApi import NorenApi

import pyalgomate.brokers.finvasia as finvasia
import pyalgomate.utils as utils
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
from pyalgomate.core import broker
from pyalgomate.core.broker import Order
from pyalgomate.strategies import OptionContract
from pyalgomate.utils import UnderlyingIndex

from . import getOptionContract, underlyingMapping

logger = logging.getLogger(__name__)


def getUnderlyingMappings():
    return underlyingMapping


def getUnderlyingDetails(underlying):
    return underlyingMapping[underlying]


def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
    underlyingDetails = getUnderlyingDetails(underlyingInstrument)
    optionPrefix = underlyingDetails["optionPrefix"]
    index = underlyingDetails["index"]

    if index not in [UnderlyingIndex.SENSEX, UnderlyingIndex.BANKEX]:
        dayMonthYear = (
            f"{expiry.day:02d}"
            + calendar.month_abbr[expiry.month].upper()
            + str(expiry.year % 100)
        )
        return optionPrefix + dayMonthYear + callOrPut + str(strikePrice)
    else:
        strikePlusOption = str(strikePrice) + (
            "CE" if (callOrPut == "C" or callOrPut == "Call") else "PE"
        )

        monthly = utils.getNearestMonthlyExpiryDate(expiry, index) == expiry

        if monthly:
            return (
                optionPrefix
                + str(expiry.year % 100)
                + calendar.month_abbr[expiry.month].upper()
                + strikePlusOption
            )
        else:
            if expiry.month == 10:
                monthlySymbol = "O"
            elif expiry.month == 11:
                monthlySymbol = "N"
            elif expiry.month == 12:
                monthlySymbol = "D"
            else:
                monthlySymbol = f"{expiry.month}"
            return (
                optionPrefix
                + str(expiry.year % 100)
                + f"{monthlySymbol}{expiry.day:02d}"
                + strikePlusOption
            )


def getOptionSymbols(underlyingInstrument, expiry, ltp, count, strikeDifference=100):
    ltp = int(float(ltp) / strikeDifference) * strikeDifference
    logger.info(f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
    optionSymbols = []
    for n in range(-count, count + 1):
        optionSymbols.append(
            getOptionSymbol(
                underlyingInstrument, expiry, ltp + (n * strikeDifference), "C"
            )
        )

    for n in range(-count, count + 1):
        optionSymbols.append(
            getOptionSymbol(
                underlyingInstrument, expiry, ltp - (n * strikeDifference), "P"
            )
        )

    logger.info("Options symbols are " + ",".join(optionSymbols))
    return optionSymbols


def getHistoricalData(
    api: NorenApi, exchangeSymbol: str, startTime: datetime.datetime, interval: str
) -> pd.DataFrame:
    startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
    splitStrings = exchangeSymbol.split("|")
    exchange = splitStrings[0]
    token = finvasia.getToken(exchangeSymbol)
    if "|" in token:
        token = token.split("|")[1]

    logger.info(f"Retrieving {interval} timeframe historical data for {exchangeSymbol}")
    ret = api.get_time_price_series(
        exchange=exchange,
        token=token,
        starttime=startTime.timestamp(),
        interval=interval,
    )
    if ret is not None:
        df = pd.DataFrame(ret)[["time", "into", "inth", "intl", "intc", "v", "oi"]]
        df = df.rename(
            columns={
                "time": "Date/Time",
                "into": "Open",
                "inth": "High",
                "intl": "Low",
                "intc": "Close",
                "v": "Volume",
                "oi": "Open Interest",
            }
        )
        df["Ticker"] = exchangeSymbol
        df[["Open", "High", "Low", "Close", "Volume", "Open Interest"]] = df[
            ["Open", "High", "Low", "Close", "Volume", "Open Interest"]
        ].astype(float)
        df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="%d-%m-%Y %H:%M:%S")
        df = df[
            [
                "Ticker",
                "Date/Time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Open Interest",
            ]
        ]
        df = df.sort_values("Date/Time")
        logger.info(f"Retrieved {df.shape[0]} rows of historical data")
        return df
    else:
        return pd.DataFrame(
            columns=[
                "Date/Time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Open Interest",
            ]
        )


def getPriceType(orderType):
    return {
        # LMT / MKT / SL-LMT / SL-MKT / DS / 2L / 3L
        broker.Order.Type.MARKET: "MKT",
        broker.Order.Type.LIMIT: "LMT",
        broker.Order.Type.STOP_LIMIT: "SL-LMT",
        broker.Order.Type.STOP: "SL-MKT",
    }.get(orderType)


class PaperTradingBroker(BacktestingBroker):
    """A Finvasia paper trading broker."""

    def __init__(self, cash, barFeed, fee=0.0025):
        super().__init__(cash, barFeed, fee)

        self.__api = barFeed.getApi()
        self.__apiAsync: NorenApiAsync = NorenApiAsync(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/",
        )
        asyncio.run(
            self.__apiAsync.set_session(
                self.__api._NorenApi__username,
                self.__api._NorenApi__password,
                self.__api._NorenApi__susertoken,
            )
        )

    def getType(self):
        return "Paper"

    def getUnderlyingMappings(self):
        return getUnderlyingMappings()

    def getUnderlyingDetails(self, underlying):
        return underlyingMapping[underlying]

    def getHistoricalData(
        self, exchangeSymbol: str, startTime: datetime.datetime, interval: str
    ) -> pd.DataFrame:
        return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def getOptionSymbol(self, underlyingInstrument, expiry, strikePrice, callOrPut):
        symbol = getUnderlyingDetails(underlyingInstrument)["optionPrefix"]

        dayMonthYear = (
            f"{expiry.day:02d}"
            + calendar.month_abbr[expiry.month].upper()
            + str(expiry.year % 100)
        )
        return (
            symbol
            + dayMonthYear
            + ("C" if (callOrPut == "C" or callOrPut == "Call") else "P")
            + str(strikePrice)
        )

    def getOptionSymbols(
        self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice
    ):
        return getOptionSymbol(
            underlyingInstrument, expiry, ceStrikePrice, "C"
        ), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, "P")

    def getOptionContract(self, symbol) -> OptionContract:
        return getOptionContract(symbol)


class OrderEvent(object):
    def __init__(self, eventDict, order):
        self.__eventDict = eventDict
        self.__order = order

    def getErrorMessage(self):
        return self.__eventDict.get("emsg", None)

    def getId(self):
        return self.__eventDict.get("norenordno", None)

    def getStatus(self):
        return self.__eventDict.get("status", None)

    def getRejectedReason(self):
        return self.__eventDict.get("rejreason", None)

    def getAvgFilledPrice(self):
        return float(self.__eventDict.get("avgprc", 0.0))

    def getTotalFilledQuantity(self):
        return float(self.__eventDict.get("fillshares", 0.0))

    def getDateTime(self):
        if "fltm" in self.__eventDict:
            return datetime.datetime.strptime(
                self.__eventDict["fltm"], "%d-%m-%Y %H:%M:%S"
            )
        elif "exch_tm" in self.__eventDict:
            return datetime.datetime.strptime(
                self.__eventDict["exch_tm"], "%d-%m-%Y %H:%M:%S"
            )
        elif "norentm" in self.__eventDict:
            return datetime.datetime.strptime(
                self.__eventDict["norentm"], "%H:%M:%S %d-%m-%Y"
            )
        else:
            return None

    def getOrder(self):
        return self.__order


LiveBroker = ForwardRef("LiveBroker")


class OrderUpdateThread(threading.Thread):
    def __init__(self, zmq_port="5555"):
        super(OrderUpdateThread, self).__init__()
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.SUB)
        self.__socket.connect(f"tcp://localhost:{zmq_port}")
        self.__socket.setsockopt_string(zmq.SUBSCRIBE, "ORDER_UPDATE")
        self.__queue = queue.Queue()
        self.__stop = False

    def getQueue(self):
        return self.__queue

    def run(self):
        while not self.__stop:
            try:
                topic, message = self.__socket.recv_multipart(flags=zmq.NOBLOCK)
                if topic == b"ORDER_UPDATE":
                    order_update = json.loads(message)
                    logger.info("Received order update: %s", order_update)
                    self.__queue.put(order_update)
            except zmq.Again:
                time.sleep(0.01)
            except Exception as e:
                logger.critical("Error retrieving ZMQ updates", exc_info=e)

    def stop(self):
        self.__stop = True
        self.__socket.close()
        self.__context.term()


class TradeMonitor(threading.Thread):
    POLL_FREQUENCY = 0.1
    RETRY_COUNT = 3
    RETRY_INTERVAL = 5

    ON_USER_TRADE = 1

    def __init__(self, liveBroker: LiveBroker):
        super(TradeMonitor, self).__init__()
        self.__broker: LiveBroker = liveBroker
        self.__queue = six.moves.queue.Queue()
        self.__stop = False
        self.__retryData = dict()
        self.__pendingUpdates = set()

        self.__zmq_update_thread = OrderUpdateThread()
        self.__zmq_update_thread.start()
        self.__stop = False

    async def processOrderEvent(self, orderEvent: OrderEvent, ret: List[OrderEvent]):
        logger.info(
            f"Processing order {orderEvent.getId()} with status {orderEvent.getStatus()}"
        )
        order = orderEvent.getOrder()

        if orderEvent.getStatus() in ["PENDING", "TRIGGER_PENDING"]:
            return

        if order not in self.__retryData:
            self.__retryData[order] = {
                "retryCount": 0,
                "lastRetryTime": time.time(),
            }

        if orderEvent.getStatus() == "OPEN":
            return

        if orderEvent.getStatus() in ["CANCELED", "REJECTED"]:
            if (
                orderEvent.getRejectedReason() is None
                or orderEvent.getRejectedReason() == "Order Cancelled"
            ):
                ret.append(orderEvent)
                self.__retryData.pop(order, None)
                return
            else:
                logger.error(
                    f"Order {orderEvent.getId()} {orderEvent.getStatus()} with reason {orderEvent.getRejectedReason()}"
                )

            retryCount = self.__retryData[order]["retryCount"]

            if retryCount < TradeMonitor.RETRY_COUNT:
                logger.warning(
                    f'Order {order.getId()} {orderEvent.getStatus()} with reason {orderEvent.getRejectedReason()}. Retrying attempt {self.__retryData[order]["retryCount"] + 1}'
                )
                self.__broker.placeOrder(order)
                self.__retryData[order]["retryCount"] += 1
                self.__retryData[order]["lastRetryTime"] = time.time()
            else:
                logger.warning(f"Exhausted retry attempts for Order {order.getId()}")
                ret.append(orderEvent)
                self.__retryData.pop(order, None)
                return
        elif orderEvent.getStatus() in ["COMPLETE"]:
            ret.append(orderEvent)
            self.__retryData.pop(order, None)
            return
        else:
            logger.error(f"Unknown trade status {orderEvent.getStatus()}")

    async def processOpenOrder(self, order: Order):
        if order not in self.__retryData:
            return

        retryCount = self.__retryData[order]["retryCount"]
        lastRetryTime = self.__retryData[order]["lastRetryTime"]

        if time.time() < (lastRetryTime + TradeMonitor.RETRY_INTERVAL):
            return

        logger.info(f"Processing open order {order.getId()}")

        # Modify the order based on current LTP for retry 0 and convert to market for retry one
        if retryCount == 0:
            ltp = self.__broker.getLastPrice(order.getInstrument())
            logger.warning(
                f"Order {order.getId()} crossed retry interval {TradeMonitor.RETRY_INTERVAL}."
                f'Retrying attempt {self.__retryData[order]["retryCount"] + 1} with current LTP {ltp}'
            )
            await self.__broker.modifyFinvasiaOrder(
                order=order,
                newprice_type=getPriceType(broker.Order.Type.LIMIT),
                newprice=ltp,
            )
        else:
            logger.warning(
                f"Order {order.getId()} crossed retry interval {TradeMonitor.RETRY_INTERVAL}."
                f'Retrying attempt {self.__retryData[order]["retryCount"] + 1} with market order'
            )
            await self.__broker.modifyFinvasiaOrder(
                order=order,
                newprice_type=getPriceType(broker.Order.Type.MARKET),
            )

        self.__retryData[order]["retryCount"] += 1
        self.__retryData[order]["lastRetryTime"] = time.time()

    def getQueue(self):
        return self.__queue

    def start(self):
        super(TradeMonitor, self).start()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.run_async())

    async def processOrderUpdate(self, order, orderUpdate, trades):
        orderEvent = OrderEvent(orderUpdate, order)
        ret: List[OrderEvent] = []
        await self.processOrderEvent(orderEvent, ret)

        if len(ret):
            self.__queue.put((TradeMonitor.ON_USER_TRADE, ret))
            trades.extend(ret)

    async def run_async(self):
        while not self.__stop:
            trades: List[OrderEvent] = []
            while True:
                try:
                    orderUpdate = self.__zmq_update_thread.getQueue().get(block=False)
                    logger.info(
                        "Pulled order %s with status %s from zmq queue",
                        orderUpdate.get("norenordno"),
                        orderUpdate.get("status"),
                    )
                    order = next(
                        (
                            o
                            for o in self.__broker.getActiveOrders().copy()
                            if o.getRemarks() == orderUpdate.get("remarks")
                        ),
                        None,
                    )
                    if order:
                        await self.processOrderUpdate(order, orderUpdate, trades)
                    else:
                        self.__pendingUpdates.add(orderUpdate)
                except queue.Empty:
                    break
                except Exception as e:
                    logger.exception(e)

            try:
                for orderUpdate in list(self.__pendingUpdates):
                    order = next(
                        (
                            o
                            for o in self.__broker.getActiveOrders().copy()
                            if o.getRemarks() == orderUpdate.get("remarks")
                        ),
                        None,
                    )
                    if order:
                        await self.processOrderUpdate(order, orderUpdate, trades)
                        self.__pendingUpdates.discard(orderUpdate)

                # Process retry logic for orders not updated via ZMQ
                for order in self.__broker.getActiveOrders().copy():
                    if order.getId() not in [event.getId() for event in trades]:
                        await self.processOpenOrder(order)  # Await the async method
            except Exception as e:
                logger.exception(e)

            await asyncio.sleep(TradeMonitor.POLL_FREQUENCY)

    def stop(self):
        self.__stop = True
        self.__zmq_update_thread.stop()
        self.__zmq_update_thread.join()


class OrderResponse(object):

    # Sample Success Response: { "request_time": "10:48:03 20-05-2020", "stat": "Ok", "norenordno": "20052000000017" }
    # Sample Success Response: { "request_time": "14:14:10 26-05-2020", "stat": "Ok", "result":"20052600000103" }
    # Sample Error Response : { "stat": "Not_Ok", "request_time": "20:40:01 19-05-2020", "emsg": "Error Occurred : 2 "invalid input"" }

    def __init__(self, response):
        self.__dict: dict = response

    def getId(self):
        return self.__dict.get("norenordno", self.__dict.get("result", None))

    def getDateTime(self):
        return datetime.datetime.strptime(
            self.__dict["request_time"], "%H:%M:%S %d-%m-%Y"
        )

    def getStat(self):
        return self.__dict.get("stat", None)

    def getErrorMessage(self):
        return self.__dict.get("emsg", None)


class LiveBroker(broker.Broker):
    """A Finvasia live broker.

    :param api: Logged in api object.
    :type api: NorenApi.

    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
        * API access permissions should include:

          * Account balance
          * Open orders
          * Buy limit order
          * User transactions
          * Cancel order
          * Sell limit order
    """

    QUEUE_TIMEOUT = 0.01

    def getType(self):
        return "Live"

    def getUnderlyingMappings(self):
        return getUnderlyingMappings()

    def getUnderlyingDetails(self, underlying):
        return underlyingMapping[underlying]

    def getOptionSymbol(self, underlyingInstrument, expiry, strikePrice, callOrPut):
        symbol = getUnderlyingDetails(underlyingInstrument)["optionPrefix"]

        dayMonthYear = (
            f"{expiry.day:02d}"
            + calendar.month_abbr[expiry.month].upper()
            + str(expiry.year % 100)
        )
        return (
            symbol
            + dayMonthYear
            + ("C" if (callOrPut == "C" or callOrPut == "Call") else "P")
            + str(strikePrice)
        )

    def getOptionSymbols(
        self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice
    ):
        return getOptionSymbol(
            underlyingInstrument, expiry, ceStrikePrice, "C"
        ), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, "P")

    def getOptionContract(self, symbol) -> OptionContract:
        return getOptionContract(symbol)

    def getHistoricalData(
        self, exchangeSymbol: str, startTime: datetime.datetime, interval: str
    ) -> pd.DataFrame:
        return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def __init__(self, api: NorenApi, barFeed: BaseBarFeed):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__api = api
        self.__apiAsync: NorenApiAsync = NorenApiAsync(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/",
        )
        asyncio.run(
            self.__apiAsync.set_session(
                self.__api._NorenApi__username,
                self.__api._NorenApi__password,
                self.__api._NorenApi__susertoken,
            )
        )
        self.__barFeed: BaseBarFeed = barFeed
        self.__tradeMonitor = TradeMonitor(self)
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders: Set[Order] = set()

    def getApi(self):
        return self.__api

    def getFeed(self):
        return self.__barFeed

    def getTradeMonitor(self):
        return self.__tradeMonitor

    def getLastPrice(self, instrument):
        ret = None
        bar = self.getFeed().getLastBar(instrument)
        if bar is not None:
            ret = bar.getPrice()
        return ret

    def getInstrumentTraits(self, instrument):
        return QuantityTraits()

    def _registerOrder(self, order: Order):
        assert order not in self.__activeOrders
        self.__activeOrders.add(order)

    def _unregisterOrder(self, order: Order):
        assert order in self.__activeOrders
        self.__activeOrders.remove(order)

    def refreshAccountBalance(self):
        try:
            logger.info("Retrieving account balance.")
            limits = self.__api.get_limits()

            if not limits or limits["stat"] != "Ok":
                logger.error(
                    f'Error retrieving account balance. Reason: {limits["emsg"]}'
                )

            marginUsed = 0

            if "marginused" in limits:
                marginUsed = float(limits["marginused"])

            self.__cash = float(limits["cash"]) - marginUsed
            logger.info(f"Available balance is <{self.__cash:.2f}>")
        except Exception:
            logger.exception(
                f'Exception retrieving account balance. Reason: {limits["emsg"]}'
            )

    def refreshOpenOrders(self):
        return

    def _startTradeMonitor(self):
        self.__stop = True  # Stop running in case of errors.
        logger.info("Initializing trade monitor.")
        self.__tradeMonitor.start()
        self.__stop = False  # No errors. Keep running.

    def _onTrade(self, order: Order, trade: OrderEvent):
        if trade.getStatus() == "REJECTED" or trade.getStatus() == "CANCELED":
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            self.notifyOrderEvent(
                broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, None)
            )
        elif trade.getStatus() == "COMPLETE":
            fee = 0
            orderExecutionInfo = broker.OrderExecutionInfo(
                trade.getAvgFilledPrice(),
                trade.getTotalFilledQuantity() - order.getFilled(),
                fee,
                trade.getDateTime(),
            )
            order.addExecutionInfo(orderExecutionInfo)
            if not order.isActive():
                self._unregisterOrder(order)
            # Notify that the order was updated.
            if order.isFilled():
                eventType = broker.OrderEvent.Type.FILLED
            else:
                eventType = broker.OrderEvent.Type.PARTIALLY_FILLED
            self.notifyOrderEvent(
                broker.OrderEvent(order, eventType, orderExecutionInfo)
            )
            logger.debug(
                f"Order filled<{order.isFilled()}> for {order.getInstrument()} at <{orderExecutionInfo.getDateTime()}>. Avg Filled Price <{orderExecutionInfo.getPrice()}>. Quantity <{orderExecutionInfo.getQuantity()}>"
            )
        else:
            logger.error(f"Unknown order status {trade.getStatus()}")

        self.refreshAccountBalance()

    def _onUserTrades(self, trades):
        for trade in trades:
            order = trade.getOrder()
            if order in self.__activeOrders:
                self._onTrade(order, trade)
            else:
                logger.info(
                    f"Trade {trade.getId()} referred to order that is not active"
                )

    # BEGIN observer.Subject interface
    def start(self):
        super(LiveBroker, self).start()
        self.refreshAccountBalance()
        self.refreshOpenOrders()
        self._startTradeMonitor()

    def stop(self):
        self.__stop = True
        logger.info("Shutting down trade monitor.")
        self.__tradeMonitor.stop()

    def join(self):
        self.__tradeMonitor.stop()
        self.__tradeMonitor.join()

    def eof(self):
        return self.__stop

    def dispatch(self):
        try:
            # Switch orders from SUBMITTED to ACCEPTED.
            ordersToProcess = list(self.__activeOrders)
            for order in ordersToProcess:
                if order.isSubmitted():
                    order.switchState(broker.Order.State.ACCEPTED)
                    self.notifyOrderEvent(
                        broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None)
                    )

            eventType, eventData = self.__tradeMonitor.getQueue().get(
                True, LiveBroker.QUEUE_TIMEOUT
            )

            if eventType == TradeMonitor.ON_USER_TRADE:
                self._onUserTrades(eventData)
            else:
                logger.error(
                    "Invalid event received to dispatch: %s - %s"
                    % (eventType, eventData)
                )
        except six.moves.queue.Empty:
            pass
        except Exception as e:
            logger.exception(e)

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # END observer.Subject interface

    # BEGIN broker.Broker interface

    def getCash(self, includeShort=True):
        return self.__cash

    def getShares(self, instrument):
        return self.__shares.get(instrument, 0)

    def getPositions(self):
        return self.__shares

    def getActiveOrder(self, orderId):
        return next((o for o in self.__activeOrders if o.getId() == orderId), None)

    def getActiveOrders(self, instrument=None):
        if instrument:
            return [o for o in self.__activeOrders if o.getInstrument() == instrument]
        return list(self.__activeOrders)

    # Place a Limit order as follows
    #     api.place_order(buy_or_sell='B', product_type='C',
    #                         exchange='NSE', tradingsymbol='INFY-EQ',
    #                         quantity=1, discloseqty=0,price_type='LMT', price=1500, trigger_price=None,
    #                         retention='DAY', remarks='my_order_001')
    # Place a Market Order as follows
    #     api.place_order(buy_or_sell='B', product_type='C',
    #                         exchange='NSE', tradingsymbol='INFY-EQ',
    #                         quantity=1, discloseqty=0,price_type='MKT', price=0, trigger_price=None,
    #                         retention='DAY', remarks='my_order_001')
    # Place a StopLoss Order as follows
    #     api.place_order(buy_or_sell='B', product_type='C',
    #                         exchange='NSE', tradingsymbol='INFY-EQ',
    #                         quantity=1, discloseqty=0,price_type='SL-LMT', price=1500, trigger_price=1450,
    #                         retention='DAY', remarks='my_order_001')
    # Place a Cover Order as follows
    #     api.place_order(buy_or_sell='B', product_type='H',
    #                         exchange='NSE', tradingsymbol='INFY-EQ',
    #                         quantity=1, discloseqty=0,price_type='LMT', price=1500, trigger_price=None,
    #                         retention='DAY', remarks='my_order_001', bookloss_price = 1490)
    # Place a Bracket Order as follows
    #     api.place_order(buy_or_sell='B', product_type='B',
    #                         exchange='NSE', tradingsymbol='INFY-EQ',
    #                         quantity=1, discloseqty=0,price_type='LMT', price=1500, trigger_price=None,
    #                         retention='DAY', remarks='my_order_001', bookloss_price = 1490, bookprofit_price = 1510)
    # Modify Order
    # Modify a New Order by providing the OrderNumber
    #     api.modify_order(exchange='NSE', tradingsymbol='INFY-EQ', orderno=orderno,
    #                                    newquantity=2, newprice_type='LMT', newprice=1505)
    # Cancel Order
    # Cancel a New Order by providing the Order Number
    #     api.cancel_order(orderno=orderno)

    async def modifyFinvasiaOrder(
        self, order: Order, newprice_type=None, newprice=0.0, newOrder: Order = None
    ):
        try:
            splitStrings = order.getInstrument().split("|")
            exchange = splitStrings[0] if len(splitStrings) > 1 else "NSE"
            symbol = splitStrings[1] if len(splitStrings) > 1 else order.getInstrument()
            quantity = order.getQuantity()

            modifyOrderResponse = await self.__apiAsync.modify_order(
                orderno=order.getId(),
                exchange=exchange,
                tradingsymbol=symbol,
                newquantity=quantity,
                newprice_type=newprice_type,
                newprice=newprice,
                newtrigger_price=None,
                bookloss_price=0.0,
                bookprofit_price=0.0,
                trail_price=0.0,
            )

            if modifyOrderResponse is None:
                raise Exception("modify_order returned None")

            ret = OrderResponse(modifyOrderResponse)

            if ret.getStat() != "Ok":
                raise Exception(ret.getErrorMessage())

            oldOrderId = order.getId()
            if oldOrderId is not None:
                self._unregisterOrder(order)

            order.setSubmitted(ret.getId(), ret.getDateTime(), order.getRemarks())
            if newOrder:
                newOrder.setSubmitted(
                    ret.getId(), ret.getDateTime(), order.getRemarks()
                )
                self._registerOrder(newOrder)
            else:
                self._registerOrder(order)

            logger.info(
                f'Modified {newprice_type} {"Buy" if order.isBuy() else "Sell"} Order {oldOrderId} with New order {order.getId()} at {order.getSubmitDateTime()}'
            )
        except Exception as e:
            logger.critical(f"Could not place order for {symbol}. Reason: {e}")

    async def placeOrder(self, order: Order):
        try:
            buyOrSell = "B" if order.isBuy() else "S"
            splitStrings = order.getInstrument().split("|")
            exchange = splitStrings[0] if len(splitStrings) > 1 else "NSE"
            # "C" For CNC, "M" FOR NRML, "I" FOR MIS, "B" FOR BRACKET ORDER, "H" FOR COVER ORDER
            productType = "I" if exchange != "BFO" else "M"
            symbol = splitStrings[1] if len(splitStrings) > 1 else order.getInstrument()
            quantity = order.getQuantity()
            price = (
                order.getLimitPrice()
                if order.getType()
                in [broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT]
                else 0
            )
            stopPrice = (
                order.getStopPrice()
                if order.getType() in [broker.Order.Type.STOP_LIMIT]
                else 0
            )
            priceType = getPriceType(order.getType())
            retention = "DAY"  # DAY / EOS / IOC
            remarks = f"PyAlgoMate order {id(order)}"

            logger.info(
                f"Placing order with buyOrSell={buyOrSell}, product_type={productType}, exchange={exchange}, "
                f"tradingsymbol={symbol}, quantity={quantity}, discloseqty=0, price_type={priceType}, "
                f"price={price}, trigger_price={stopPrice}, retention={retention}, remarks={remarks}"
            )
            placedOrderResponse = await self.__apiAsync.place_order(
                buy_or_sell=buyOrSell,
                product_type=productType,
                exchange=exchange,
                tradingsymbol=symbol,
                quantity=quantity,
                discloseqty=0,
                price_type=priceType,
                price=price,
                trigger_price=stopPrice,
                retention=retention,
                remarks=remarks,
            )

            if placedOrderResponse is None:
                raise Exception("place_order returned None")

            orderResponse = OrderResponse(placedOrderResponse)

            if orderResponse.getStat() != "Ok":
                raise Exception(orderResponse.getErrorMessage())

            oldOrderId = order.getId()
            if oldOrderId is not None:
                self._unregisterOrder(order)

            order.setSubmitted(
                orderResponse.getId(), orderResponse.getDateTime(), remarks
            )

            self._registerOrder(order)

            logger.info(
                f'Placed {priceType} {"Buy" if order.isBuy() else "Sell"} Order {oldOrderId} New order {order.getId()} at {order.getSubmitDateTime()}'
            )
        except Exception as e:
            logger.critical(f"Could not place order for {symbol}. Reason: {e}")

    async def submitOrder(self, order: Order):
        if order.isInitial():
            # Override user settings based on Finvasia limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            await self.placeOrder(order)

            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            order.switchState(broker.Order.State.SUBMITTED)
        else:
            raise Exception("The order was already processed")

    async def modifyOrder(self, oldOrder: Order, newOrder: Order):
        if newOrder.isInitial():
            newOrder.setAllOrNone(False)
            newOrder.setGoodTillCanceled(True)

            await self.modifyFinvasiaOrder(
                order=oldOrder,
                newprice_type=getPriceType(newOrder.getType()),
                newprice=newOrder.getLimitPrice(),
                newOrder=newOrder,
            )

            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            newOrder.switchState(broker.Order.State.SUBMITTED)
        else:
            raise Exception("The order was already processed")

    def _createOrder(self, orderType, action, instrument, quantity, price, stopPrice):
        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY: broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT: broker.Order.Action.SELL,
            broker.Order.Action.SELL: broker.Order.Action.SELL,
        }.get(action, None)

        if action is None:
            raise Exception("Only BUY/SELL orders are supported")

        if orderType == broker.MarketOrder:
            return broker.MarketOrder(
                action,
                instrument,
                quantity,
                False,
                self.getInstrumentTraits(instrument),
            )
        elif orderType == broker.LimitOrder:
            return broker.LimitOrder(
                action,
                instrument,
                price,
                quantity,
                self.getInstrumentTraits(instrument),
            )
        elif orderType == broker.StopOrder:
            return broker.StopOrder(
                action,
                instrument,
                stopPrice,
                quantity,
                self.getInstrumentTraits(instrument),
            )
        elif orderType == broker.StopLimitOrder:
            return broker.StopLimitOrder(
                action,
                instrument,
                stopPrice,
                price,
                quantity,
                self.getInstrumentTraits(instrument),
            )

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        return self._createOrder(
            broker.MarketOrder, action, instrument, quantity, None, None
        )

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return self._createOrder(
            broker.LimitOrder, action, instrument, quantity, limitPrice, None
        )

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return self._createOrder(
            broker.StopOrder, action, instrument, quantity, None, stopPrice
        )

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return self._createOrder(
            broker.StopLimitOrder, action, instrument, quantity, limitPrice, stopPrice
        )

    async def cancelOrder(self, order: Order):
        if order not in self.__activeOrders:
            raise Exception("The order is not active anymore")
        if order.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        try:
            cancelOrderResponse = await self.__apiAsync.cancel_order(
                orderno=order.getId()
            )

            if cancelOrderResponse is None:
                raise Exception("cancel_order returned None")

            orderResponse = OrderResponse(cancelOrderResponse)

            if orderResponse.getStat() != "Ok":
                raise Exception(orderResponse.getErrorMessage())

            logger.debug(
                f"Canceled order {orderResponse.getId()} at {orderResponse.getDateTime()}"
            )
        except Exception as e:
            logger.critical(f"Could not cancel order for {order.getId()}. Reason: {e}")

    # END broker.Broker interface
