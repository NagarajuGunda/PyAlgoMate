"""
.. moduleauthor:: Nagaraju Gunda
"""
import threading
import time
import logging
import datetime
import six
import calendar
import re
import pandas as pd

from .kiteext import KiteExt

from pyalgotrade import broker
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
from pyalgomate.strategies import OptionContract
import pyalgomate.utils as utils
from pyalgomate.utils import UnderlyingIndex

logger = logging.getLogger(__file__)

underlyingMapping = {
    'NSE:NIFTY MID SELECT': {
        'optionPrefix': 'NFO:MIDCPNIFTY',
        'index': UnderlyingIndex.MIDCPNIFTY,
        'lotSize': 75,
        'strikeDifference': 25
    },
    'NSE:NIFTY BANK': {
        'optionPrefix': 'NFO:BANKNIFTY',
        'index': UnderlyingIndex.BANKNIFTY,
        'lotSize': 15,
        'strikeDifference': 100
    },
    'NSE:NIFTY 50': {
        'optionPrefix': 'NFO:NIFTY',
        'index': UnderlyingIndex.NIFTY,
        'lotSize': 25,
        'strikeDifference': 50
    },
    'NSE:NIFTY FIN SERVICE': {
        'optionPrefix': 'NFO:FINNIFTY',
        'index': UnderlyingIndex.FINNIFTY,
        'lotSize': 40,
        'strikeDifference': 50
    },
    'BSE:SENSEX': {
        'optionPrefix': 'BFO:SENSEX',
        'index': UnderlyingIndex.SENSEX,
        'lotSize': 10,
        'strikeDifference': 100
    },
    'BSE:BANKEX': {
        'optionPrefix': 'BFO:BANKEX',
        'index': UnderlyingIndex.BANKEX,
        'lotSize': 15,
        'strikeDifference': 100
    }
}


def getUnderlyingMappings():
    return underlyingMapping


def getUnderlyingDetails(underlying):
    return underlyingMapping[underlying]


def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
    monthly = utils.getNearestMonthlyExpiryDate(expiry) == expiry
    symbol = getUnderlyingDetails(underlyingInstrument)['optionPrefix']

    strikePlusOption = str(strikePrice) + ('CE' if (callOrPut ==
                                                    'C' or callOrPut == 'Call') else 'PE')
    if monthly:
        return symbol + str(expiry.year % 100) + calendar.month_abbr[expiry.month].upper() + strikePlusOption
    else:
        if expiry.month == 10:
            monthlySymbol = 'O'
        elif expiry.month == 11:
            monthlySymbol = 'N'
        elif expiry.month == 12:
            monthlySymbol = 'D'
        else:
            monthlySymbol = f'{expiry.month}'
        return symbol + str(expiry.year % 100) + f"{monthlySymbol}{expiry.day:02d}" + strikePlusOption


def getOptionSymbols(underlyingInstrument, expiry, ltp, count, strikeDifference=100):
    ltp = int(float(ltp) / strikeDifference) * strikeDifference
    logger.info(f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
    optionSymbols = []
    for n in range(-count, count + 1):
        optionSymbols.append(getOptionSymbol(
            underlyingInstrument, expiry, ltp + (n * strikeDifference), 'C'))

    for n in range(-count, count + 1):
        optionSymbols.append(getOptionSymbol(
            underlyingInstrument, expiry, ltp - (n * strikeDifference), 'P'))

    logger.info("Options symbols are " + ",".join(optionSymbols))
    return optionSymbols


def getZerodhaTokensList(api: KiteExt, instruments):
    tokenMappings = {}
    response = api.ltp(instruments)
    for instrument in instruments:
        try:
            token = response[instrument]['instrument_token']
            tokenMappings[token] = instrument
        except:
            logger.warn(f"Token mapping faile for {instrument}")
    return tokenMappings


def getHistoricalData(api, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
    startTime = startTime.replace(
        hour=0, minute=0, second=0, microsecond=0)
    splitStrings = exchangeSymbol.split(':')
    exchange = splitStrings[0]

    tokensList = getZerodhaTokensList(api, [exchangeSymbol])
    token = next(iter(tokensList))

    logger.info(
        f'Retrieving {interval} timeframe historical data for {exchangeSymbol}')
    ret = api.historical_data(
        token, startTime, datetime.datetime.now(), interval=f'{interval if interval != "1" else ""}minute', oi=True)

    if ret != None:
        df = pd.DataFrame(
            ret)[['date', 'open', 'high', 'low', 'close', 'volume', 'oi']]
        df = df.rename(columns={'date': 'Date/Time', 'open': 'Open', 'high': 'High',
                                'low': 'Low', 'close': 'Close', 'volume': 'Volume', 'oi': 'Open Interest'})

        df[['Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']] = df[[
            'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']].astype(float)

        df = df.sort_values('Date/Time')

        logger.info(f'Retrieved {df.shape[0]} rows of historical data')
        return df
    else:
        return pd.DataFrame(columns=['Date/Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest'])


class ZerodhaPaperTradingBroker(BacktestingBroker):
    """A Zerodha paper trading broker.
    """

    def __init__(self, cash, barFeed, fee=0.0025):
        super().__init__(cash, barFeed, fee)

        self.__api = barFeed.getApi()

    def getType(self):
        return "Paper"

    def getUnderlyingMappings(self):
        return getUnderlyingMappings()

    def getUnderlyingDetails(self, underlying):
        return underlyingMapping[underlying]

    def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
        return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def getOptionSymbol(self, underlyingInstrument, expiry: datetime.date, strikePrice, callOrPut):
        return getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument,
                                                                                                  expiry, peStrikePrice,
                                                                                                  'P')

    def getOptionContract(self, symbol):
        m = re.match(r"([A-Z\:]+)(\d{2})([A-Z]{3})(\d+)([CP])E", symbol)

        if m is not None:
            month = datetime.datetime.strptime(m.group(3), '%b').month
            year = int(m.group(2)) + 2000
            expiry = utils.getNearestMonthlyExpiryDate(
                datetime.date(year, month, 1))
            optionPrefix = m.group(1)
            for underlying, underlyingDetails in underlyingMapping.items():
                if underlyingDetails['optionPrefix'] == optionPrefix:
                    return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p",
                                          underlying)

        m = re.match(r"([A-Z\:]+)(\d{2})(\d|[OND])(\d{2})(\d+)([CP])E", symbol)

        if m is None:
            return None

        day = int(m.group(4))
        month = m.group(3)
        if month == 'O':
            month = 10
        elif month == 'N':
            month = 11
        elif month == 'D':
            month = 12
        else:
            month = int(month)

        year = int(m.group(2)) + 2000
        expiry = datetime.date(year, month, day)
        optionPrefix = m.group(1)
        for underlying, underlyingDetails in underlyingMapping.items():
            if underlyingDetails['optionPrefix'] == optionPrefix:
                return OptionContract(symbol, int(m.group(5)), expiry, "c" if m.group(6) == "C" else "p", underlying)


class TradeEvent(object):
    def __init__(self, eventDict):
        self.__eventDict = eventDict

    def getId(self):
        return self.__eventDict.get('order_id', None)

    def getStatus(self):
        return self.__eventDict.get('status', None)

    def getRejectedReason(self):
        return self.__eventDict.get('status_message', None)

    def getAvgFilledPrice(self):
        return float(self.__eventDict.get('average_price', 0.0))

    def getTotalFilledQuantity(self):
        return float(self.__eventDict.get('filled_quantity', 0.0))

    def getDateTime(self):
        return self.__eventDict['order_timestamp'] if self.__eventDict.get('order_timestamp',
                                                                           None) is not None else None


class TradeMonitor(threading.Thread):
    POLL_FREQUENCY = 2

    # Events
    ON_USER_TRADE = 1

    def __init__(self, liveBroker: broker.Broker):
        super(TradeMonitor, self).__init__()
        self.__api = liveBroker.getApi()
        self.__broker = liveBroker
        self.__queue = six.moves.queue.Queue()
        self.__stop = False

    def _getNewTrades(self):
        ret = []
        activeOrderIds = [order.getId()
                          for order in self.__broker.getActiveOrders().copy()]
        if len(activeOrderIds) == 0:
            return ret

        orderBook = None
        try:
            orderBook = self.__api.orders()
        except Exception as e:
            logger.error('Failed to fetch order book', )
            return ret

        for orderId in activeOrderIds:
            for bOrder in orderBook:
                foundOrder = None
                if orderId == bOrder['order_id']:
                    foundOrder = bOrder
                    break

            if foundOrder is not None:
                logger.info(f'Found order for orderId {orderId}')

                if foundOrder['status'] in ['OPEN', 'PENDING']:
                    continue
                elif foundOrder['status'] in ['CANCELLED', 'REJECTED', 'COMPLETE']:
                    ret.append(TradeEvent(foundOrder))
                else:
                    logger.error(
                        f'Unknown trade status {foundOrder.get("status", None)}')

        # Sort by time, so older trades first.
        return sorted(ret, key=lambda t: t.getDateTime())

    def getQueue(self):
        return self.__queue

    def start(self):
        trades = self._getNewTrades()
        if len(trades):
            logger.info(
                f'Last trade found at {trades[-1].getDateTime()}. Order id {trades[-1].getId()}')

        super(TradeMonitor, self).start()

    def run(self):
        while not self.__stop:
            try:
                trades = self._getNewTrades()
                if len(trades):
                    logger.info(f'{len(trades)} new trade/s found')
                    self.__queue.put((TradeMonitor.ON_USER_TRADE, trades))
            except Exception as e:
                logger.critical(
                    "Error retrieving user transactions", exc_info=e)

            time.sleep(TradeMonitor.POLL_FREQUENCY)

    def stop(self):
        self.__stop = True


class ZerodhaLiveBroker(broker.Broker):
    """A Zerodha live broker.

    :param api: Logged in api object.
    :type api: ShoonyaApi.

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

    def getOptionSymbol(self, underlyingInstrument, expiry: datetime.date, strikePrice, callOrPut):
        return getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument,
                                                                                                  expiry, peStrikePrice,
                                                                                                  'P')

    def getOptionContract(self, symbol):
        m = re.match(r"([A-Z\:]+)(\d{2})([A-Z]{3})(\d+)([CP])E", symbol)

        if m is not None:
            month = datetime.datetime.strptime(m.group(3), '%b').month
            year = int(m.group(2)) + 2000
            expiry = utils.getNearestMonthlyExpiryDate(
                datetime.date(year, month, 1))
            optionPrefix = m.group(1)
            for underlying, underlyingDetails in underlyingMapping.items():
                if underlyingDetails['optionPrefix'] == optionPrefix:
                    return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p",
                                          underlying)

        m = re.match(r"([A-Z\:]+)(\d{2})(\d|[OND])(\d{2})(\d+)([CP])E", symbol)

        if m is None:
            return None

        day = int(m.group(4))
        month = m.group(3)
        if month == 'O':
            month = 10
        elif month == 'N':
            month = 11
        elif month == 'D':
            month = 12
        else:
            month = int(month)

        year = int(m.group(2)) + 2000
        expiry = datetime.date(year, month, day)
        optionPrefix = m.group(1)
        for underlying, underlyingDetails in underlyingMapping.items():
            if underlyingDetails['optionPrefix'] == optionPrefix:
                return OptionContract(symbol, int(m.group(5)), expiry, "c" if m.group(6) == "C" else "p", underlying)

    def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
        return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def __init__(self, api: KiteExt):
        super(ZerodhaLiveBroker, self).__init__()
        self.__stop = False
        self.__api = api
        self.__tradeMonitor = TradeMonitor(self)
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders = {}

    def getApi(self):
        return self.__api

    def getInstrumentTraits(self, instrument):
        return QuantityTraits()

    def _registerOrder(self, order):
        assert (order.getId() not in self.__activeOrders)
        assert (order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert (order.getId() in self.__activeOrders)
        assert (order.getId() is not None)
        del self.__activeOrders[order.getId()]

    def _startTradeMonitor(self):
        self.__stop = True  # Stop running in case of errors.
        logger.info("Initializing trade monitor.")
        self.__tradeMonitor.start()
        self.__stop = False  # No errors. Keep running.

    def _onTrade(self, order, trade):
        if trade.getStatus() == 'REJECTED' or trade.getStatus() == 'CANCELLED':
            if trade.getRejectedReason() is not None:
                logger.error(
                    f'Order {trade.getId()} rejected with reason {trade.getRejectedReason()}')
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            self.notifyOrderEvent(broker.OrderEvent(
                order, broker.OrderEvent.Type.CANCELED, None))
        elif trade.getStatus() == 'COMPLETE':
            fee = 0
            orderExecutionInfo = broker.OrderExecutionInfo(
                trade.getAvgFilledPrice(), trade.getTotalFilledQuantity() - order.getFilled(), fee, trade.getDateTime())
            order.addExecutionInfo(orderExecutionInfo)
            if not order.isActive():
                self._unregisterOrder(order)
            # Notify that the order was updated.
            if order.isFilled():
                eventType = broker.OrderEvent.Type.FILLED
            else:
                eventType = broker.OrderEvent.Type.PARTIALLY_FILLED
            self.notifyOrderEvent(broker.OrderEvent(
                order, eventType, orderExecutionInfo))
        else:
            logger.error(f'Unknown order status {trade.getStatus()}')

    def _onUserTrades(self, trades):
        for trade in trades:
            order = self.__activeOrders.get(trade.getId())
            if order is not None:
                self._onTrade(order, trade)
            else:
                logger.info(
                    f"Trade {trade.getId()} refered to order that is not active")

    # BEGIN observer.Subject interface
    def start(self):
        super(ZerodhaLiveBroker, self).start()
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
        # Switch orders from SUBMITTED to ACCEPTED.
        ordersToProcess = list(self.__activeOrders.values())
        for order in ordersToProcess:
            if order.isSubmitted():
                order.switchState(broker.Order.State.ACCEPTED)
                self.notifyOrderEvent(broker.OrderEvent(
                    order, broker.OrderEvent.Type.ACCEPTED, None))

        # Dispatch events from the trade monitor.
        try:
            eventType, eventData = self.__tradeMonitor.getQueue().get(
                True, ZerodhaLiveBroker.QUEUE_TIMEOUT)

            if eventType == TradeMonitor.ON_USER_TRADE:
                self._onUserTrades(eventData)
            else:
                logger.error(
                    "Invalid event received to dispatch: %s - %s" % (eventType, eventData))
        except six.moves.queue.Empty:
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

    def getActiveOrders(self, instrument=None):
        return list(self.__activeOrders.values())

    def __placeOrder(self, buyOrSell, productType, exchange, symbol, quantity, price, priceType, triggerPrice, remarks):
        try:
            return self.__api.place_order(variety=self.__api.VARIETY_REGULAR,
                                          tradingsymbol=symbol,
                                          exchange=exchange,
                                          transaction_type=buyOrSell,
                                          quantity=quantity,
                                          order_type=priceType,
                                          product=productType,
                                          price=price,
                                          trigger_price=triggerPrice,
                                          tag=remarks)
        except Exception as e:
            raise Exception(e)

    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Zerodha limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            buyOrSell = self.__api.TRANSACTION_TYPE_BUY if order.isBuy(
            ) else self.__api.TRANSACTION_TYPE_SELL
            # MIS, NRML, CNC, BO or CO
            productType = self.__api.PRODUCT_MIS
            splitStrings = order.getInstrument().split(':')
            exchange = splitStrings[0] if len(splitStrings) > 1 else 'NSE'
            symbol = splitStrings[1] if len(
                splitStrings) > 1 else order.getInstrument()
            quantity = int(order.getQuantity())
            price = order.getLimitPrice() if order.getType() in [
                broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT] else 0
            stopPrice = order.getStopPrice() if order.getType() in [
                broker.Order.Type.STOP_LIMIT] else 0
            priceType = {
                # MARKET, LIMIT, SL or SL-M.
                broker.Order.Type.MARKET: 'MARKET',
                broker.Order.Type.LIMIT: 'LIMIT',
                broker.Order.Type.STOP_LIMIT: 'SL',
                broker.Order.Type.STOP: 'SL-M'
            }.get(order.getType())

            logger.info(
                f'Placing {priceType} {"Buy" if order.isBuy() else "Sell"} order for {order.getInstrument()} with {quantity} quantity')
            try:
                orderId = self.__placeOrder(buyOrSell,
                                            productType,
                                            exchange,
                                            symbol,
                                            quantity,
                                            price,
                                            priceType,
                                            stopPrice,
                                            None)
            except Exception as e:
                logger.critical(
                    f'Could not place order for {symbol}. Error {e}')
                return

            dateTime = datetime.datetime.now()
            logger.info(
                f'Placed {priceType} {"Buy" if order.isBuy() else "Sell"} order {orderId} at {dateTime}')
            order.setSubmitted(orderId,
                               dateTime)
            self._registerOrder(order)
            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            order.switchState(broker.Order.State.SUBMITTED)
        else:
            raise Exception("The order was already processed")

    def _createOrder(self, orderType, action, instrument, quantity, price, stopPrice):
        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY: broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT: broker.Order.Action.SELL,
            broker.Order.Action.SELL: broker.Order.Action.SELL
        }.get(action, None)

        if action is None:
            raise Exception("Only BUY/SELL orders are supported")

        if orderType == broker.MarketOrder:
            return broker.MarketOrder(action, instrument, quantity, False, self.getInstrumentTraits(instrument))
        elif orderType == broker.LimitOrder:
            return broker.LimitOrder(action, instrument, price, quantity, self.getInstrumentTraits(instrument))
        elif orderType == broker.StopOrder:
            return broker.StopOrder(action, instrument, stopPrice, quantity, self.getInstrumentTraits(instrument))
        elif orderType == broker.StopLimitOrder:
            return broker.StopLimitOrder(action, instrument, stopPrice, price, quantity,
                                         self.getInstrumentTraits(instrument))

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        return self._createOrder(broker.MarketOrder, action, instrument, quantity, None, None)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return self._createOrder(broker.LimitOrder, action, instrument, quantity, limitPrice, None)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return self._createOrder(broker.StopOrder, action, instrument, quantity, None, stopPrice)

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return self._createOrder(broker.StopLimitOrder, action, instrument, quantity, limitPrice, stopPrice)

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self.__api.cancel_order(orderno=order.getId())
        self._unregisterOrder(order)
        order.switchState(broker.Order.State.CANCELED)

        # Notify that the order was canceled.
        self.notifyOrderEvent(broker.OrderEvent(
            order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))

    # END broker.Broker interface
