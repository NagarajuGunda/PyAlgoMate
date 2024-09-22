"""
.. moduleauthor:: Nagaraju Gunda
"""

import asyncio
import calendar
import datetime
import json
import logging
import os
import tempfile
import threading
import time
import traceback
from typing import ForwardRef, Set

import pandas as pd
import zmq
from NorenRestApiAsync.NorenApiAsync import NorenApiAsync
from NorenRestApiPy.NorenApi import NorenApi

import pyalgomate.brokers.finvasia as finvasia
import pyalgomate.utils as utils
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
from pyalgomate.core import broker
from pyalgomate.core.broker import Order
from pyalgomate.core.dispatcher import LiveAsyncDispatcher
from pyalgomate.strategies import OptionContract
from pyalgomate.utils import UnderlyingIndex

from . import getOptionContract, underlyingMapping

logger = logging.getLogger(__name__)
scripMasterDf: pd.DataFrame = finvasia.getScriptMaster()

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


def getFutureSymbol(underlyingIndex: UnderlyingIndex, expiry: datetime.date):
    futureRow = scripMasterDf[
        (scripMasterDf["Expiry"].dt.date == expiry)
        & (scripMasterDf["Instrument"] == "FUTIDX")
        & scripMasterDf["TradingSymbol"].str.startswith(str(underlyingIndex))
    ].iloc[0]
    return futureRow["Exchange"] + "|" + futureRow["TradingSymbol"]


class PaperTradingBroker(BacktestingBroker):
    """A Finvasia paper trading broker."""

    def __init__(self, cash, barFeed, fee=0.0025):
        super().__init__(cash, barFeed, fee)

        self.__api = barFeed.getApi()
        self.loop = LiveAsyncDispatcher().loop
        self.__apiAsync: NorenApiAsync = NorenApiAsync(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/",
        )
        asyncio.run_coroutine_threadsafe(
            self.__apiAsync.set_session(
                self.__api._NorenApi__username,
                self.__api._NorenApi__password,
                self.__api._NorenApi__susertoken,
            ),
            self.loop,
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

    def getFutureSymbol(self, underlyingIndex: UnderlyingIndex, expiry: datetime.date):
        return getFutureSymbol(underlyingIndex, expiry)


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


class TradeMonitor(threading.Thread):
    POLL_FREQUENCY = 0.01
    RETRY_COUNT = 2
    RETRY_INTERVAL = 0.5

    def __init__(self, liveBroker: LiveBroker, ipc_path=None, max_concurrent_tasks=100):
        super(TradeMonitor, self).__init__()
        self.__broker: LiveBroker = liveBroker
        self.__stop = False
        self.__retryData = dict()
        self.__pendingUpdates = set()
        self.__openOrders = set()
        self.__semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.__zmq_context = zmq.asyncio.Context()
        self.__zmq_socket = self.__zmq_context.socket(zmq.SUB)

        if ipc_path is None:
            # Create a platform-independent IPC path
            ipc_dir = tempfile.gettempdir()
            ipc_file = "pyalgomate_ipc"
            self.__ipc_path = os.path.join(ipc_dir, ipc_file)
        else:
            self.__ipc_path = ipc_path

        # On Windows, we need to use tcp instead of ipc
        if os.name == "nt":
            self.__zmq_socket.connect(
                f"tcp://127.0.0.1:{self.__ipc_path.split(':')[-1]}"
            )
        else:
            self.__zmq_socket.connect(f"ipc://{self.__ipc_path}")

        self.__zmq_socket.setsockopt_string(zmq.SUBSCRIBE, "ORDER_UPDATE")
        self.__zmq_socket.setsockopt(zmq.RCVHWM, 1000)
        self.__zmq_socket.setsockopt(zmq.RCVBUF, 1024 * 1024)
        self.__order_queue = asyncio.Queue()

    async def run_async(self):
        asyncio.create_task(self.process_zmq_updates())
        asyncio.create_task(self.process_order_queue())
        while not self.__stop:
            try:
                await self.process_pending_updates()
                await self.process_open_orders()
            except Exception as e:
                logger.exception("Error in TradeMonitor run loop", exc_info=e)
            await asyncio.sleep(self.POLL_FREQUENCY)

    async def process_zmq_updates(self):
        while not self.__stop:
            try:
                [topic, message] = await self.__zmq_socket.recv_multipart()
                if topic == b"ORDER_UPDATE":
                    order_update = json.loads(message)
                    await self.__order_queue.put(order_update)
            except Exception as e:
                logger.exception("Error retrieving ZMQ updates", exc_info=e)

    async def process_order_queue(self):
        while not self.__stop:
            order_update = await self.__order_queue.get()
            asyncio.create_task(self.__handle_order_update_with_semaphore(order_update))

    async def __handle_order_update_with_semaphore(self, order_update):
        try:
            async with self.__semaphore:
                await self.handle_order_update(order_update)
        except Exception as e:
            logger.exception("Error handling order update", exc_info=e)

    async def handle_order_update(self, order_update):
        status = order_update.get("status")
        if status in ["PENDING", "TRIGGER_PENDING"]:
            return

        order = self.find_order(order_update)
        if order:
            await self.process_order_update(order, order_update)
        else:
            self.__pendingUpdates.add(frozenset(order_update.items()))

    def find_order(self, order_update):
        return self.__broker.getActiveOrder(order_update.get("norenordno", None))

    async def process_order_update(self, order: Order, order_update: dict):
        if order not in self.__retryData:
            self.__retryData[order] = {"retryCount": 0, "lastRetryTime": time.time()}

        status = order_update.get("status")
        order_event = OrderEvent(order_update, order)

        if status == "OPEN":
            self.__openOrders.add(order)
        elif status in ["CANCELED", "REJECTED"]:
            await self.handle_canceled_rejected(order, order_event)
        elif status == "COMPLETE":
            await self.__broker._onUserTrades([order_event])
            self.__retryData.pop(order, None)
            self.__openOrders.discard(order)
        else:
            logger.error(f"Unknown order status {status}")

    async def handle_canceled_rejected(self, order: Order, order_event: OrderEvent):
        if (
            order_event.getRejectedReason() is None
            or order_event.getRejectedReason() == "Order Cancelled"
        ):
            await self.__broker._onUserTrades([order_event])
            self.__retryData.pop(order, None)
            self.__openOrders.discard(order)
        else:
            await self.retry_order(order, order_event)

    async def retry_order(self, order: Order, order_event: OrderEvent):
        retry_data = self.__retryData[order]
        if retry_data["retryCount"] < self.RETRY_COUNT:
            logger.warning(
                f"Order {order.getId()} {order_event.getStatus()} with reason {order_event.getRejectedReason()}. "
                f"Retrying attempt {retry_data['retryCount'] + 1}"
            )
            await self.__broker.placeOrder(order)
            retry_data["retryCount"] += 1
            retry_data["lastRetryTime"] = time.time()
        else:
            logger.warning(f"Exhausted retry attempts for Order {order.getId()}")
            await self.__broker._onUserTrades([order_event])
            self.__retryData.pop(order, None)
            self.__openOrders.discard(order)

    async def process_pending_updates(self):
        for update in list(self.__pendingUpdates):
            order_update = dict(update)
            order = self.find_order(order_update)
            if order:
                await self.handle_order_update(order_update)
                self.__pendingUpdates.remove(update)

    async def process_open_orders(self):
        for order in list(self.__openOrders):
            await self.check_and_retry_open_order(order)

    async def check_and_retry_open_order(self, order: Order):
        retry_data = self.__retryData[order]
        if time.time() < (retry_data["lastRetryTime"] + self.RETRY_INTERVAL):
            return

        retry_count = retry_data["retryCount"]
        if retry_count < self.RETRY_COUNT:
            await self.retry_open_order(order, retry_count)
        else:
            logger.warning(f"Exhausted retry attempts for open Order {order.getId()}")
            self.__retryData.pop(order, None)
            self.__openOrders.discard(order)

    async def retry_open_order(self, order: Order, retry_count: int):
        try:
            if retry_count == self.RETRY_COUNT - 1:
                logger.warning(
                    f"Final retry for Order {order.getId()}. Attempting market order."
                )
                await self.__broker.modifyFinvasiaOrder(
                    order=order, newprice_type=getPriceType(broker.Order.Type.MARKET)
                )
            else:
                ltp = self.__broker.getLastPrice(order.getInstrument())
                logger.warning(f"Retrying Order {order.getId()} with current LTP {ltp}")
                await self.__broker.modifyFinvasiaOrder(
                    order=order,
                    newprice_type=getPriceType(broker.Order.Type.LIMIT),
                    newprice=ltp,
                )

            self.__retryData[order]["retryCount"] += 1
            self.__retryData[order]["lastRetryTime"] = time.time()
        except Exception as e:
            logger.exception(f"Error retrying open order {order.getId()}", exc_info=e)

    def start(self):
        super(TradeMonitor, self).start()

    def run(self):
        asyncio.run_coroutine_threadsafe(self.run_async(), self.__broker.loop)

    def stop(self):
        self.__stop = True
        self.__zmq_socket.close()
        self.__zmq_context.term()


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
        self.loop = LiveAsyncDispatcher().loop
        self.__apiAsync: NorenApiAsync = NorenApiAsync(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/",
        )
        asyncio.run_coroutine_threadsafe(
            self.__apiAsync.set_session(
                self.__api._NorenApi__username,
                self.__api._NorenApi__password,
                self.__api._NorenApi__susertoken,
            ),
            self.loop,
        )
        self.__barFeed: BaseBarFeed = barFeed
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders: Set[Order] = set()

        self.__tradeMonitor = TradeMonitor(self)

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
        # Switch order from SUBMITTED to ACCEPTED
        if order.isSubmitted():
            order.switchState(broker.Order.State.ACCEPTED)
            self.notifyOrderEvent(
                broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None)
            )

        if trade.getStatus() == "REJECTED":
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            errorMsg = trade.getRejectedReason()
            orderExecutionInfo = broker.OrderExecutionInfo(
                None, 0, 0, trade.getDateTime(), error=errorMsg
            )
            order.addExecutionInfo(orderExecutionInfo)
            self.notifyOrderEvent(
                broker.OrderEvent(
                    order, broker.OrderEvent.Type.CANCELED, orderExecutionInfo
                )
            )
        elif trade.getStatus() == "CANCELED":
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            orderExecutionInfo = broker.OrderExecutionInfo(
                None, 0, 0, trade.getDateTime()
            )
            order.addExecutionInfo(orderExecutionInfo)
            self.notifyOrderEvent(
                broker.OrderEvent(
                    order, broker.OrderEvent.Type.CANCELED, orderExecutionInfo
                )
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

    async def _onUserTrades(self, trades):
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
        self.__tradeMonitor.join()

    def join(self):
        self.__tradeMonitor.stop()
        self.__tradeMonitor.join()

    def eof(self):
        return self.__stop

    def dispatch(self):
        # Do nothing in the dispatch method
        pass

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
        self,
        order: Order,
        newprice_type=None,
        newprice=0.0,
        newtrigger_price=0.0,
        newOrder: Order = None,
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
                newtrigger_price=newtrigger_price,
                bookloss_price=0.0,
                bookprofit_price=0.0,
                trail_price=0.0,
            )

            if modifyOrderResponse is None:
                raise Exception("modify_order returned None")

            ret = OrderResponse(modifyOrderResponse)

            if ret.getStat() != "Ok":
                logger.error(
                    f"modify order failed. Full API response: {modifyOrderResponse}"
                )
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
            logger.critical(f"Could not modify order for {symbol}. Reason: {e}")

    async def placeOrder(self, order: Order):
        infoMsg = ""
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

            infoMsg = f"buyOrSell={buyOrSell}, product_type={productType}, exchange={exchange}, "
            f"tradingsymbol={symbol}, quantity={quantity}, discloseqty=0, price_type={priceType}, "
            f"price={price}, trigger_price={stopPrice}, retention={retention}, remarks={remarks}"
            logger.info(f"Placing order with {infoMsg}")
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
                logger.error(f"place order failed. Full API response: {orderResponse}")
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
            logger.critical(
                f"Could not place order for {symbol}. Reason: {e}\nOrder Info: {infoMsg}"
            )
            logger.error(traceback.print_exc())

    async def submitOrder(self, order: Order):
        if order.isInitial():
            # Override user settings based on Finvasia limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            order.switchState(broker.Order.State.SUBMITTED)

            await self.placeOrder(order)
        else:
            raise Exception("The order was already processed")

    async def modifyOrder(self, oldOrder: Order, newOrder: Order):
        if newOrder.isInitial():
            newOrder.setAllOrNone(False)
            newOrder.setGoodTillCanceled(True)

            newTriggerPrice = (
                newOrder.getStopPrice()
                if newOrder.getType() in [broker.Order.Type.STOP_LIMIT]
                else None
            )

            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            newOrder.switchState(broker.Order.State.SUBMITTED)

            await self.modifyFinvasiaOrder(
                order=oldOrder,
                newprice_type=getPriceType(newOrder.getType()),
                newprice=newOrder.getLimitPrice(),
                newtrigger_price=newTriggerPrice,
                newOrder=newOrder,
            )
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
                logger.error(f"Cancel order failed. Full API response: {orderResponse}")
                raise Exception(orderResponse.getErrorMessage())

            logger.debug(
                f"Canceled order {orderResponse.getId()} at {orderResponse.getDateTime()}"
            )
        except Exception as e:
            logger.critical(f"Could not cancel order for {order.getId()}. Reason: {e}")

    def getFutureSymbol(self, underlyingIndex: UnderlyingIndex, expiry: datetime.date):
        return getFutureSymbol(underlyingIndex, expiry)

    # END broker.Broker interface
