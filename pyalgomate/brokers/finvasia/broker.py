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
from typing import ForwardRef, List, Dict

from pyalgotrade import broker
from pyalgotrade.broker import Order
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
from pyalgomate.strategies import OptionContract
from NorenRestApiPy.NorenApi import NorenApi
from pyalgomate.utils import UnderlyingIndex
import pyalgomate.utils as utils
import pyalgomate.brokers.finvasia as finvasia

logger = logging.getLogger(__name__)

underlyingMapping = {
    'NSE|MIDCPNIFTY': {
        'optionPrefix': 'NFO|MIDCPNIFTY',
        'index': UnderlyingIndex.MIDCPNIFTY,
        'lotSize': 75,
        'strikeDifference': 25
    },
    'NSE|NIFTY BANK': {
        'optionPrefix': 'NFO|BANKNIFTY',
        'index': UnderlyingIndex.BANKNIFTY,
        'lotSize': 15,
        'strikeDifference': 100
    },
    'NSE|NIFTY INDEX': {
        'optionPrefix': 'NFO|NIFTY',
        'index': UnderlyingIndex.NIFTY,
        'lotSize': 25,
        'strikeDifference': 50
    },
    'NSE|FINNIFTY': {
        'optionPrefix': 'NFO|FINNIFTY',
        'index': UnderlyingIndex.FINNIFTY,
        'lotSize': 40,
        'strikeDifference': 50
    },
    'BSE|SENSEX': {
        'optionPrefix': 'BFO|SENSEX',
        'index': UnderlyingIndex.SENSEX,
        'lotSize': 10,
        'strikeDifference': 100
    },
    'BSE|BANKEX': {
        'optionPrefix': 'BFO|BANKEX',
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
    underlyingDetails = getUnderlyingDetails(underlyingInstrument)
    optionPrefix = underlyingDetails['optionPrefix']
    index = underlyingDetails['index']

    if index not in [UnderlyingIndex.SENSEX, UnderlyingIndex.BANKEX]:
        dayMonthYear = f"{expiry.day:02d}" + \
            calendar.month_abbr[expiry.month].upper() + str(expiry.year % 100)
        return optionPrefix + dayMonthYear + callOrPut + str(strikePrice)
    else:
        strikePlusOption = str(strikePrice) + ('CE' if (callOrPut ==
                                                        'C' or callOrPut == 'Call') else 'PE')

        monthly = utils.getNearestMonthlyExpiryDate(expiry, index) == expiry

        if monthly:
            return optionPrefix + str(expiry.year % 100) + calendar.month_abbr[expiry.month].upper() + strikePlusOption
        else:
            if expiry.month == 10:
                monthlySymbol = 'O'
            elif expiry.month == 11:
                monthlySymbol = 'N'
            elif expiry.month == 12:
                monthlySymbol = 'D'
            else:
                monthlySymbol = f'{expiry.month}'
            return optionPrefix + str(expiry.year % 100) + f"{monthlySymbol}{expiry.day:02d}" + strikePlusOption


def getOptionSymbols(underlyingInstrument, expiry, ltp, count, strikeDifference=100):
    ltp = int(float(ltp) / strikeDifference) * strikeDifference
    logger.info(f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
    optionSymbols = []
    for n in range(-count, count+1):
       optionSymbols.append(getOptionSymbol(
           underlyingInstrument, expiry, ltp + (n * strikeDifference), 'C'))

    for n in range(-count, count+1):
       optionSymbols.append(getOptionSymbol(
           underlyingInstrument, expiry, ltp - (n * strikeDifference), 'P'))

    logger.info("Options symbols are " + ",".join(optionSymbols))
    return optionSymbols

def getHistoricalData(api: NorenApi, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame:
    startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
    splitStrings = exchangeSymbol.split('|')
    exchange = splitStrings[0]
    token=finvasia.getToken(exchangeSymbol)
    if '|' in token :
        token = token.split('|')[1]

    logger.info(
        f'Retrieving {interval} timeframe historical data for {exchangeSymbol}')
    ret = api.get_time_price_series(exchange=exchange, token=token, starttime=startTime.timestamp(), interval=interval)
    if ret != None:
        df = pd.DataFrame(
            ret)[['time', 'into', 'inth', 'intl', 'intc', 'v', 'oi']]
        df = df.rename(columns={'time': 'Date/Time', 'into': 'Open', 'inth': 'High',
                                'intl': 'Low', 'intc': 'Close', 'v': 'Volume', 'oi': 'Open Interest'})
        df['Ticker'] = exchangeSymbol
        df[['Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']] = df[[
            'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']].astype(float)
        df['Date/Time'] = pd.to_datetime(df['Date/Time'], format="%d-%m-%Y %H:%M:%S")
        df = df[['Ticker', 'Date/Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']]
        df = df.sort_values('Date/Time')
        logger.info(f'Retrieved {df.shape[0]} rows of historical data')
        return df
    else:
        return pd.DataFrame(columns=['Date/Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest'])

def getPriceType(orderType):
    return {
                # LMT / MKT / SL-LMT / SL-MKT / DS / 2L / 3L
                broker.Order.Type.MARKET: 'MKT',
                broker.Order.Type.LIMIT: 'LMT',
                broker.Order.Type.STOP_LIMIT: 'SL-LMT',
                broker.Order.Type.STOP: 'SL-MKT'
            }.get(orderType)
class PaperTradingBroker(BacktestingBroker):
    """A Finvasia paper trading broker.
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

    def getOptionSymbol(self, underlyingInstrument, expiry, strikePrice, callOrPut):
        symbol = getUnderlyingDetails(underlyingInstrument)['optionPrefix']

        dayMonthYear = f"{expiry.day:02d}" + \
            calendar.month_abbr[expiry.month].upper() + str(expiry.year % 100)
        return symbol + dayMonthYear + ('C' if (callOrPut == 'C' or callOrPut == 'Call') else 'P') + str(strikePrice)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, 'P')

    def getOptionContract(self, symbol) -> OptionContract:
        m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", symbol)

        if m is None:
            m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d+)([CP])E", symbol)

            if m is not None:
                optionPrefix = m.group(1)
                for underlying, underlyingDetails in underlyingMapping.items():
                    if underlyingDetails['optionPrefix'] == optionPrefix:
                        index = self.getUnderlyingDetails(underlying)['index']
                        month = datetime.datetime.strptime(m.group(3), '%b').month
                        year = int(m.group(2)) + 2000
                        expiry = utils.getNearestMonthlyExpiryDate(
                            datetime.date(year, month, 1), index)
                        return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p", underlying)

            m = re.match(r"([A-Z\|]+)(\d{2})(\d|[OND])(\d{2})(\d+)([CP])E", symbol)

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

        day = int(m.group(2))
        month = m.group(3)
        year = int(m.group(4)) + 2000
        expiry = datetime.date(
            year, datetime.datetime.strptime(month, '%b').month, day)

        optionPrefix = m.group(1)

        for underlying, underlyingDetails in underlyingMapping.items():
            if underlyingDetails['optionPrefix'] == optionPrefix:
                return OptionContract(symbol, int(m.group(6)), expiry, "c" if m.group(5) == "C" else "p", underlying)

    pass

    # get_order_book
    # Response data will be in json Array of objects with below fields in case of success.

    # Json Fields	Possible value	Description
    # stat	        Ok or Not_Ok	Order book success or failure indication.
    # exch		                    Exchange Segment
    # tsym		                    Trading symbol / contract on which order is placed.
    # norenordno		            Noren Order Number
    # prc		                    Order Price
    # qty		                    Order Quantity
    # prd		                    Display product alias name, using prarr returned in user details.
    # status
    # trantype	    B / S       	Transaction type of the order
    # prctyp	    LMT / MKT   	Price type
    # fillshares	                Total Traded Quantity of this order
    # avgprc		                Average trade price of total traded quantity
    # rejreason		                If order is rejected, reason in text form
    # exchordid		                Exchange Order Number
    # cancelqty		                Canceled quantity for order which is in status cancelled.
    # remarks		                Any message Entered during order entry.
    # dscqty		                Order disclosed quantity.
    # trgprc		                Order trigger price
    # ret           DAY / IOC / EOS	Order validity
    # uid
    # actid
    # bpprc		                    Book Profit Price applicable only if product is selected as B (Bracket order )
    # blprc		                    Book loss Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # trailprc	                    Trailing Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # amo		                    Yes / No
    # pp		                    Price precision
    # ti		                    Tick size
    # ls		                    Lot size
    # token		                    Contract Token
    # norentm
    # ordenttm
    # exch_tm
    # snoordt		                0 for profit leg and 1 for stoploss leg
    # snonum		                This field will be present for product H and B; and only if it is profit/sl order.

    # Response data will be in json format with below fields in case of failure:

    # Json Fields	Possible value	Description
    # stat	        Not_Ok	        Order book failure indication.
    # request_time		            Response received time.
    # emsg		                    Error message

    # single_order_history

    # Json Fields	Possible value	Description
    # stat	        Ok or Not_Ok	Order book success or failure indication.
    # exch		                    Exchange Segment
    # tsym		                    Trading symbol / contract on which order is placed.
    # norenordno		            Noren Order Number
    # prc		                    Order Price
    # qty		                    Order Quantity
    # prd		                    Display product alias name, using prarr returned in user details.
    # status
    # rpt		                    (fill/complete etc)
    # trantype	    B / S	        Transaction type of the order
    # prctyp	    LMT / MKT	    Price type
    # fillshares	                Total Traded Quantity of this order
    # avgprc		                Average trade price of total traded quantity
    # rejreason		                If order is rejected, reason in text form
    # exchordid		                Exchange Order Number
    # cancelqty		                Canceled quantity for order which is in status cancelled.
    # remarks		                Any message Entered during order entry.
    # dscqty		                Order disclosed quantity.
    # trgprc		                Order trigger price
    # ret	        DAY / IOC / EOS	Order validity
    # uid
    # actid
    # bpprc		                    Book Profit Price applicable only if product is selected as B (Bracket order )
    # blprc		                    Book loss Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # trailprc	                    	Trailing Price applicable only if product is selected as H and B (High Leverage and Bracket order )
    # amo		                    Yes / No
    # pp		                    Price precision
    # ti		                    Tick size
    # ls		                    Lot size
    # token		                    Contract Token
    # norentm
    # ordenttm
    # exch_tm
    #
    # Response data will be in json format with below fields in case of failure:

    # Json Fields	Possible value	Description
    # stat	        Not_Ok	        Order book failure indication.
    # request_time		            Response received time.
    # emsg		                    Error message


class OrderEvent(object):
    def __init__(self, eventDict):
        self.__eventDict = eventDict

    def getStat(self):
        return self.__eventDict.get('state', None)
    
    def getErrorMessage(self):
        return self.__eventDict.get('emsg', None)

    def getId(self):
        return self.__eventDict.get('norenordno', None)

    def getStatus(self):
        return self.__eventDict.get('status', None)

    def getRejectedReason(self):
        return self.__eventDict.get('rejreason', None)

    def getAvgFilledPrice(self):
        return float(self.__eventDict.get('avgprc', 0.0))

    def getTotalFilledQuantity(self):
        return float(self.__eventDict.get('fillshares', 0.0))

    def getDateTime(self):
        return datetime.datetime.strptime(self.__eventDict['norentm'], '%H:%M:%S %d-%m-%Y') if self.__eventDict.get('norentm', None) is not None else None

LiveBroker = ForwardRef('LiveBroker')

class TradeMonitor(threading.Thread):
    POLL_FREQUENCY = 1

    RETRY_COUNT = 3

    RETRY_INTERVAL = 5

    ON_USER_TRADE = 1

    def __init__(self, liveBroker: LiveBroker):
        super(TradeMonitor, self).__init__()
        self.__api: NorenApi = liveBroker.getApi()
        self.__broker: LiveBroker = liveBroker
        self.__queue = six.moves.queue.Queue()
        self.__stop = False
        self.__retryData = dict()

    def getNewTrades(self):
        ret: List[OrderEvent] = []
        activeOrders: List[Order] = [order for order in self.__broker.getActiveOrders().copy()]
        orderBook = self.__api.get_order_book()
        for order in activeOrders:
            filteredOrders = [orderBookOrder for orderBookOrder in orderBook if orderBookOrder.get('norenordno') == order.getId()]
            if len(filteredOrders) == 0:
                logger.warning(f'Order not found in the order book for order id {order.getId()}')
                continue

            orderEvent = OrderEvent(filteredOrders[0])

            if order not in self.__retryData:
                self.__retryData[order] = {'retryCount': 0, 'lastRetryTime': time.time()}

            if orderEvent.getStat() == 'Not_Ok':
                logger.error(f'Fetching order history for {orderEvent.getId()} failed with reason {orderEvent.getErrorMessage()}')
                continue
            elif orderEvent.getStatus() in ['PENDING', 'TRIGGER_PENDING']:
                pass
            elif orderEvent.getStatus() == 'OPEN':
                retryCount = self.__retryData[order]['retryCount']
                lastRetryTime = self.__retryData[order]['lastRetryTime']

                if time.time() < (lastRetryTime + TradeMonitor.RETRY_INTERVAL):
                    continue

                # Modify the order based on current LTP for retry 0 and convert to market for retry one
                if retryCount == 0:
                    ltp = self.__broker.getLastPrice(order.getInstrument())
                    logger.warning(f'Order {order.getId()} crossed retry interval {TradeMonitor.RETRY_INTERVAL}.'
                                   f'Retrying attempt {self.__retryData[order]["retryCount"] + 1} with current LTP {ltp}')
                    self.__broker.modifyOrder(order=order, newprice_type=getPriceType(broker.Order.Type.LIMIT), newprice=ltp)
                else:
                    logger.warning(f'Order {order.getId()} crossed retry interval {TradeMonitor.RETRY_INTERVAL}.'
                                   f'Retrying attempt {self.__retryData[order]["retryCount"] + 1} with market order')
                    self.__broker.modifyOrder(order=order, newprice_type=getPriceType(broker.Order.Type.MARKET))

                self.__retryData[order]['retryCount'] += 1
                self.__retryData[order]['lastRetryTime'] = time.time()
            elif orderEvent.getStatus() in ['CANCELED', 'REJECTED']:
                if orderEvent.getRejectedReason() is None or orderEvent.getRejectedReason() == 'Order Cancelled':
                    ret.append(orderEvent)
                    self.__retryData.pop(order, None)
                    continue
                else:
                    logger.error(
                        f'Order {orderEvent.getId()} {orderEvent.getStatus()} with reason {orderEvent.getRejectedReason()}')

                retryCount = self.__retryData[order]['retryCount']
                lastRetryTime = self.__retryData[order]['lastRetryTime']

                if retryCount < TradeMonitor.RETRY_COUNT:
                    if time.time() > (lastRetryTime + TradeMonitor.RETRY_INTERVAL):
                        logger.warning(f'Order {order.getId()} {orderEvent.getStatus()} with reason {orderEvent.getRejectedReason()}. Retrying attempt {self.__retryData[order]["retryCount"] + 1}')
                        self.__broker.placeOrder(order)
                        self.__retryData[order]['retryCount'] += 1
                        self.__retryData[order]['lastRetryTime'] = time.time()
                else:
                    logger.warning(f'Exhausted retry attempts for Order {order.getId()}')
                    ret.append(orderEvent)
                    self.__retryData.pop(order, None)
            elif orderEvent.getStatus() in ['COMPLETE']:
                ret.append(orderEvent)
                self.__retryData.pop(order, None)
            else:
                logger.error(f'Unknown trade status {orderEvent.getStatus()}')

        # Sort by time, so older trades first.
        return sorted(ret, key=lambda t: t.getDateTime())

    def getQueue(self):
        return self.__queue

    def start(self):
        trades = self.getNewTrades()
        if len(trades):
            logger.info(
                f'Last trade found at {trades[-1].getDateTime()}. Order id {trades[-1].getId()}')
            self.__queue.put((TradeMonitor.ON_USER_TRADE, trades))

        super(TradeMonitor, self).start()

    def run(self):
        while not self.__stop:
            try:
                trades = self.getNewTrades()
                if len(trades):
                    logger.info(f'{len(trades)} new trade/s found')
                    self.__queue.put((TradeMonitor.ON_USER_TRADE, trades))
            except Exception as e:
                logger.critical(
                    "Error retrieving user transactions", exc_info=e)

            time.sleep(TradeMonitor.POLL_FREQUENCY)

    def stop(self):
        self.__stop = True


class OrderResponse(object):

    # Sample Success Response: { "request_time": "10:48:03 20-05-2020", "stat": "Ok", "norenordno": "20052000000017" }
    # Sample Success Response: { "request_time": "14:14:10 26-05-2020", "stat": "Ok", "result":"20052600000103" }
    # Sample Error Response : { "stat": "Not_Ok", "request_time": "20:40:01 19-05-2020", "emsg": "Error Occurred : 2 "invalid input"" }

    def __init__(self, response):
        self.__dict: dict = response

    def getId(self):
        return self.__dict.get('norenordno', self.__dict.get('result', None))

    def getDateTime(self):
        return datetime.datetime.strptime(self.__dict["request_time"], "%H:%M:%S %d-%m-%Y")

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
        symbol = getUnderlyingDetails(underlyingInstrument)['optionPrefix']

        dayMonthYear = f"{expiry.day:02d}" + \
            calendar.month_abbr[expiry.month].upper() + str(expiry.year % 100)
        return symbol + dayMonthYear + ('C' if (callOrPut == 'C' or callOrPut == 'Call') else 'P') + str(strikePrice)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, 'P')

    def getOptionContract(self, symbol) -> OptionContract:
        m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", symbol)

        if m is None:
            m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d+)([CP])E", symbol)

            if m is not None:
                optionPrefix = m.group(1)
                for underlying, underlyingDetails in underlyingMapping.items():
                    if underlyingDetails['optionPrefix'] == optionPrefix:
                        index = self.getUnderlyingDetails(underlying)['index']
                        month = datetime.datetime.strptime(m.group(3), '%b').month
                        year = int(m.group(2)) + 2000
                        expiry = utils.getNearestMonthlyExpiryDate(
                            datetime.date(year, month, 1), index)
                        return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p", underlying)

            m = re.match(r"([A-Z\|]+)(\d{2})(\d|[OND])(\d{2})(\d+)([CP])E", symbol)

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

        day = int(m.group(2))
        month = m.group(3)
        year = int(m.group(4)) + 2000
        expiry = datetime.date(
            year, datetime.datetime.strptime(month, '%b').month, day)

        optionPrefix = m.group(1)

        for underlying, underlyingDetails in underlyingMapping.items():
            if underlyingDetails['optionPrefix'] == optionPrefix:
                return OptionContract(symbol, int(m.group(6)), expiry, "c" if m.group(5) == "C" else "p", underlying)

    def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
        return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def __init__(self, api: NorenApi, barFeed: BaseBarFeed):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__api: NorenApi = api
        self.__barFeed: BaseBarFeed = barFeed
        self.__tradeMonitor = TradeMonitor(self)
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders: Dict[str, Order] = dict()

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
        assert (order.getId() not in self.__activeOrders)
        assert (order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order: Order):
        assert (order.getId() in self.__activeOrders)
        assert (order.getId() is not None)
        del self.__activeOrders[order.getId()]

    def refreshAccountBalance(self):
        try:
            logger.info("Retrieving account balance.")
            limits = self.__api.get_limits()

            if not limits or limits['stat'] != 'Ok':
                logger.error(
                    f'Error retrieving account balance. Reason: {limits["emsg"]}')

            marginUsed = 0

            if 'marginused' in limits:
                marginUsed = float(limits['marginused'])

            self.__cash = float(limits['cash']) - marginUsed
            logger.info(f'Available balance is <{self.__cash:.2f}>')
        except Exception as e:
            logger.exception(
                f'Exception retrieving account balance. Reason: {limits["emsg"]}')

    def refreshOpenOrders(self):
        return
        self.__stop = True  # Stop running in case of errors.
        logger.info("Retrieving open orders.")
        openOrders = None  # self.__api.getOpenOrders()
        # for openOrder in openOrders:
        #     self._registerOrder(build_order_from_open_order(
        #         openOrder, self.getInstrumentTraits(common.btc_symbol)))

        logger.info("%d open order/s found" % (len(openOrders)))
        self.__stop = False  # No errors. Keep running.

    def _startTradeMonitor(self):
        self.__stop = True  # Stop running in case of errors.
        logger.info("Initializing trade monitor.")
        self.__tradeMonitor.start()
        self.__stop = False  # No errors. Keep running.

    def _onTrade(self, order: Order, trade: OrderEvent):
        if trade.getStatus() == 'REJECTED' or trade.getStatus() == 'CANCELED':
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
            logger.info(f'Order filled<{order.isFilled()}> for {order.getInstrument()} at <{orderExecutionInfo.getDateTime()}>. Avg Filled Price <{orderExecutionInfo.getPrice()}>. Quantity <{orderExecutionInfo.getQuantity()}>')
        else:
            logger.error(f'Unknown order status {trade.getStatus()}')

        self.refreshAccountBalance()

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
            ordersToProcess = list(self.__activeOrders.values())
            for order in ordersToProcess:
                if order.isSubmitted():
                    order.switchState(broker.Order.State.ACCEPTED)
                    self.notifyOrderEvent(broker.OrderEvent(
                        order, broker.OrderEvent.Type.ACCEPTED, None))

            eventType, eventData = self.__tradeMonitor.getQueue().get(
                True, LiveBroker.QUEUE_TIMEOUT)

            if eventType == TradeMonitor.ON_USER_TRADE:
                self._onUserTrades(eventData)
            else:
                logger.error(
                    "Invalid event received to dispatch: %s - %s" % (eventType, eventData))
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
        return self.__activeOrders.get(orderId)

    def getActiveOrders(self, instrument=None):
        return list(self.__activeOrders.values())

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

    def modifyOrder(self, order: Order, newprice_type=None, newprice=0.0):
        try:
            splitStrings = order.getInstrument().split('|')
            exchange = splitStrings[0] if len(splitStrings) > 1 else 'NSE'
            symbol = splitStrings[1] if len(
                splitStrings) > 1 else order.getInstrument()
            quantity = order.getQuantity()

            modifyOrderResponse = self.__api.modify_order(orderno=order.getId(),
                                                    exchange=exchange,
                                                    tradingsymbol=symbol,
                                                    newquantity=quantity,
                                                    newprice_type=newprice_type,
                                                    newprice=newprice,
                                                    newtrigger_price=None,
                                                    bookloss_price = 0.0,
                                                    bookprofit_price = 0.0,
                                                    trail_price = 0.0)

            if modifyOrderResponse is None:
                raise Exception('modify_order returned None')

            ret = OrderResponse(modifyOrderResponse)

            if ret.getStat() != "Ok":
                raise Exception(ret.getErrorMessage())
            
            oldOrderId = order.getId()
            if oldOrderId is not None:
                self._unregisterOrder(order)
            
            order.setSubmitted(ret.getId(),
                                ret.getDateTime())
            self._registerOrder(order)

            logger.info(
                f'Modified {newprice_type} {"Buy" if order.isBuy() else "Sell"} Order {oldOrderId} with New order {order.getId()} at {order.getSubmitDateTime()}')
        except Exception as e:
            logger.critical(f'Could not place order for {symbol}. Reason: {e}')

    def placeOrder(self, order: Order):
        try:
            buyOrSell = 'B' if order.isBuy() else 'S'
            splitStrings = order.getInstrument().split('|')
            exchange = splitStrings[0] if len(splitStrings) > 1 else 'NSE'
            # "C" For CNC, "M" FOR NRML, "I" FOR MIS, "B" FOR BRACKET ORDER, "H" FOR COVER ORDER
            productType = 'I' if exchange != 'BFO' else 'M'
            symbol = splitStrings[1] if len(
                splitStrings) > 1 else order.getInstrument()
            quantity = order.getQuantity()
            price = order.getLimitPrice() if order.getType() in [
                broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT] else 0
            stopPrice = order.getStopPrice() if order.getType() in [
                broker.Order.Type.STOP_LIMIT] else 0
            priceType = getPriceType(order.getType())
            retention = 'DAY'  # DAY / EOS / IOC

            logger.info(
                f'Placing order with buyOrSell={buyOrSell}, product_type={productType}, exchange={exchange}, '
                f'tradingsymbol={symbol}, quantity={quantity}, discloseqty=0, price_type={priceType}, '
                f'price={price}, trigger_price={stopPrice}, retention={retention}, remarks="PyAlgoMate order"')
            placedOrderResponse = self.__api.place_order(buy_or_sell=buyOrSell, product_type=productType,
                                                    exchange=exchange, tradingsymbol=symbol,
                                                    quantity=quantity, discloseqty=0, price_type=priceType,
                                                    price=price, trigger_price=stopPrice,
                                                    retention=retention, remarks=f'PyAlgoMate order')

            if placedOrderResponse is None:
                raise Exception('place_order returned None')

            orderResponse = OrderResponse(placedOrderResponse)

            if orderResponse.getStat() != "Ok":
                raise Exception(orderResponse.getErrorMessage())
            
            oldOrderId = order.getId()
            if oldOrderId is not None:
                self._unregisterOrder(order)
            
            order.setSubmitted(orderResponse.getId(),
                                orderResponse.getDateTime())

            self._registerOrder(order)

            logger.info(
                f'Placed {priceType} {"Buy" if order.isBuy() else "Sell"} Order {oldOrderId} New order {order.getId()} at {order.getSubmitDateTime()}')            
        except Exception as e:
            logger.critical(f'Could not place order for {symbol}. Reason: {e}')

    def submitOrder(self, order: Order):
        if order.isInitial():
            # Override user settings based on Finvasia limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            self.placeOrder(order)

            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            order.switchState(broker.Order.State.SUBMITTED)
        else:
            raise Exception("The order was already processed")

    def _createOrder(self, orderType, action, instrument, quantity, price, stopPrice):
        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY:          broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT:   broker.Order.Action.SELL,
            broker.Order.Action.SELL:         broker.Order.Action.SELL
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
            return broker.StopLimitOrder(action, instrument, stopPrice, price, quantity, self.getInstrumentTraits(instrument))

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        return self._createOrder(broker.MarketOrder, action, instrument, quantity, None, None)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return self._createOrder(broker.LimitOrder, action, instrument, quantity, limitPrice, None)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return self._createOrder(broker.StopOrder, action, instrument, quantity, None, stopPrice)

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return self._createOrder(broker.StopLimitOrder, action, instrument, quantity, limitPrice, stopPrice)

    def cancelOrder(self, order: Order):
        activeOrder: Order = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        try:
            cancelOrderResponse = self.__api.cancel_order(orderno=order.getId())

            if cancelOrderResponse is None:
                raise Exception('cancel_order returned None')

            orderResponse = OrderResponse(cancelOrderResponse)

            if orderResponse.getStat() != "Ok":
                raise Exception(orderResponse.getErrorMessage())

            logger.info(f'Canceled order {orderResponse.getId()} at {orderResponse.getDateTime()}')
        except Exception as e:
            logger.critical(f'Could not cancel order for {order.getId()}. Reason: {e}')

    # END broker.Broker interface
