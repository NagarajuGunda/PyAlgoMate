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

from pyalgotrade import broker
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
from pyalgomate.strategies import OptionContract
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logger = logging.getLogger(__file__)


def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
    symbol = 'NFO|NIFTY'
    if 'NIFTY BANK' in underlyingInstrument:
        symbol = 'NFO|BANKNIFTY'

    dayMonthYear = f"{expiry.day:02d}" + \
        calendar.month_abbr[expiry.month].upper() + str(expiry.year % 100)
    return symbol + dayMonthYear + callOrPut + str(strikePrice)


def getOptionSymbols(underlyingInstrument, expiry, ltp, count):
    ltp = int(float(ltp) / 100) * 100
    logger.info(f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
    optionSymbols = []
    for n in range(-count, count+1):
       optionSymbols.append(getOptionSymbol(
           underlyingInstrument, expiry, ltp + (n * 100), 'C'))

    for n in range(-count, count+1):
       optionSymbols.append(getOptionSymbol(
           underlyingInstrument, expiry, ltp - (n * 100), 'P'))

    logger.info("Options symbols are " + ",".join(optionSymbols))
    return optionSymbols


def getFinvasiaToken(api, exchangeSymbol):
    splitStrings = exchangeSymbol.split('|')
    exchange = splitStrings[0]
    symbol = splitStrings[1]
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)

    if ret != None:
        for value in ret['values']:
            if value['instname'] in ['OPTIDX', 'EQ'] and value['tsym'] == symbol:
                return value['token']
            if value['instname'] == 'UNDIND' and value['cname'] == symbol:
                return value['token']

    return None


def getFinvasiaTokenMappings(api, exchangeSymbols):
    tokenMappings = {}

    for exchangeSymbol in exchangeSymbols:
        tokenMappings["{0}|{1}".format(exchangeSymbol.split(
            '|')[0], getFinvasiaToken(api, exchangeSymbol))] = exchangeSymbol

    return tokenMappings


def getHistoricalData(api, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
    startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
    splitStrings = exchangeSymbol.split('|')
    exchange = splitStrings[0]

    logger.info(
        f'Retrieving {interval} timeframe historical data for {exchangeSymbol}')
    ret = api.get_time_price_series(exchange=exchange, token=getFinvasiaToken(
        api, exchangeSymbol), starttime=startTime.timestamp(), interval=interval)
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

class PaperTradingBroker(BacktestingBroker):
    """A Finvasia paper trading broker.
    """    
    def __init__(self, cash, barFeed, fee=0.0025):
        super().__init__(cash, barFeed, fee)
        
        self.__api = barFeed.getApi()

    def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
        return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def getOptionSymbol(self, underlyingInstrument, expiry, strikePrice, callOrPut):
        symbol = 'NFO|NIFTY'
        if 'NIFTY BANK' in underlyingInstrument or 'BANKNIFTY' in underlyingInstrument:
            symbol = 'NFO|BANKNIFTY'

        dayMonthYear = f"{expiry.day:02d}" + \
            calendar.month_abbr[expiry.month].upper() + str(expiry.year % 100)
        return symbol + dayMonthYear + ('C' if (callOrPut == 'C' or callOrPut == 'Call') else 'P') + str(strikePrice)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, 'P')

    def getOptionContract(self, symbol) -> OptionContract:
        m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", symbol)

        if m is None:
            return None

        day = int(m.group(2))
        month = m.group(3)
        year = int(m.group(4)) + 2000
        expiry = datetime.date(
            year, datetime.datetime.strptime(month, '%b').month, day)
        return OptionContract(symbol, int(m.group(6)), expiry, "c" if m.group(5) == "C" else "p", m.group(1).replace('NFO|BANKNIFTY', 'NSE|NIFTY BANK').replace('NFO|NIFTY', 'NSE|NIFTY INDEX'))

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


class TradeEvent(object):
    def __init__(self, eventDict):
        self.__eventDict = eventDict

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

# def getOrderStatus(orderId):
#     orderBook = api.get_order_book()
#     for order in orderBook:
#         if order['norenordno'] == orderId and order['status'] == 'REJECTED':
#             return item['rejreason']
#         elif order['norenordno'] == orderId and order['status'] == 'OPEN':
#             return item['status']
#         elif order['norenordno'] == orderId and order['status'] == 'COMPLETE':
#             print(f'{orderId} successfully placed')
#             return item['status']
#     print(f'{orderId} not found in the order book')
#     return None

# def getFillPrice(orderId):
#     tradeBook = api.get_trade_book()
#     if tradeBook is None:
#         print('No order placed for the day')
#     else:
#         for trade in tradeBook:
#             if trade['norenordno'] == orderId:
#                 return trade['flprc']
#     return None


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
        for orderId in activeOrderIds:
            orderHistories = self.__api.single_order_history(
                orderno=orderId)
            if orderHistories is None:
                logger.info(
                    f'Order history not found for order id {orderId}')
                continue

            for orderHistory in orderHistories:
                if orderHistory['stat'] == 'Not_Ok':
                    errorMsg = orderHistory['emsg']
                    logger.error(
                        f'Fetching order history for {orderId} failed with with reason {errorMsg}')
                    continue
                elif orderHistory['status'] in ['OPEN', 'PENDING', 'TRIGGER_PENDING']:
                    continue
                elif orderHistory['status'] in ['CANCELED', 'REJECTED', 'COMPLETE']:
                    ret.append(TradeEvent(orderHistory))
                else:
                    logger.error(
                        f'Unknown trade status {orderHistory.get("status", None)}')

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


class OrderResponse(object):

    # Sample Success Response: { "request_time": "10:48:03 20-05-2020", "stat": "Ok", "norenordno": "20052000000017" }
    # Sample Error Response : { "stat": "Not_Ok", "request_time": "20:40:01 19-05-2020", "emsg": "Error Occurred : 2 "invalid input"" }

    def __init__(self, dict):
        self.__dict = dict

    def getId(self):
        return self.__dict["norenordno"]

    def getDateTime(self):
        return datetime.datetime.strptime(self.__dict["request_time"], "%H:%M:%S %d-%m-%Y")

    def getStat(self):
        return self.__dict.get("stat", None)

    def getErrorMessage(self):
        return self.__dict.get("emsg", None)


class LiveBroker(broker.Broker):
    """A Finvasia live broker.
    
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

    def getOptionSymbol(self, underlyingInstrument, expiry, strikePrice, callOrPut):
        symbol = 'NFO|NIFTY'
        if 'NIFTY BANK' in underlyingInstrument or 'BANKNIFTY' in underlyingInstrument:
            symbol = 'NFO|BANKNIFTY'

        dayMonthYear = f"{expiry.day:02d}" + \
            calendar.month_abbr[expiry.month].upper() + str(expiry.year % 100)
        return symbol + dayMonthYear + ('C' if (callOrPut == 'C' or callOrPut == 'Call') else 'P') + str(strikePrice)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, 'P')

    def getOptionContract(self, symbol) -> OptionContract:
        m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", symbol)

        if m is None:
            return None

        day = int(m.group(2))
        month = m.group(3)
        year = int(m.group(4)) + 2000
        expiry = datetime.date(
            year, datetime.datetime.strptime(month, '%b').month, day)
        return OptionContract(symbol, int(m.group(6)), expiry, "c" if m.group(5) == "C" else "p", m.group(1).replace('NFO|BANKNIFTY', 'NSE|NIFTY BANK').replace('NFO|NIFTY', 'NSE|NIFTY INDEX'))

    def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
        return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def __init__(self, api: ShoonyaApi):
        super(LiveBroker, self).__init__()
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

    def refreshAccountBalance(self):
        self.__stop = True  # Stop running in case of errors.
        logger.info("Retrieving account balance.")
        balance = self.__api.get_limits()

        # Cash
        # self.__cash = round(balance.getUSDAvailable(), 2)
        # logger.info("%s USD" % (self.__cash))
        # # BTC
        # btc = balance.getBTCAvailable()
        # if btc:
        #     self.__shares = {common.btc_symbol: btc}
        # else:
        #     self.__shares = {}
        # logger.info("%s BTC" % (btc))

        self.__stop = False  # No errors. Keep running.

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

    def _onTrade(self, order, trade):
        if trade.getStatus() == 'REJECTED' or trade.getStatus() == 'CANCELED':
            if trade.getRejectedReason() is not None:
                logger.error(
                    f'Order {trade.getId()} rejected with reason {trade.getRejectedReason()}')
            self._unregisterOrder(order)
            order.switchState(broker.Order.State.CANCELED)
            self.notifyOrderEvent(broker.OrderEvent(
                order, broker.OrderEvent.Type.CANCELED, None))
        # elif trade.getStatus() == 'OPEN':
        #     order.switchState(broker.Order.State.ACCEPTED)
        #     self.notifyOrderEvent(broker.OrderEvent(
        #         order, broker.OrderEvent.Type.ACCEPTED, None))
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
        super(LiveBroker, self).start()
        self.refreshAccountBalance()
        self.refreshOpenOrders()
        self._startTradeMonitor()

    def stop(self):
        self.__stop = True
        logger.info("Shutting down trade monitor.")
        self.__tradeMonitor.stop()

    def join(self):
        if self.__tradeMonitor.isAlive():
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
                True, LiveBroker.QUEUE_TIMEOUT)

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

    def __placeOrder(self, buyOrSell, productType, exchange, symbol, quantity, price, priceType, triggerPrice, retention, remarks):
        try:
            orderResponse = self.__api.place_order(buy_or_sell=buyOrSell, product_type=productType,
                                                   exchange=exchange, tradingsymbol=symbol,
                                                   quantity=quantity, discloseqty=0, price_type=priceType,
                                                   price=price, trigger_price=triggerPrice,
                                                   retention=retention, remarks=remarks)
        except Exception as e:
            raise Exception(e)

        if orderResponse is None:
            raise Exception('place_order returned None')

        ret = OrderResponse(orderResponse)

        if ret.getStat() != "Ok":
            raise Exception(ret.getErrorMessage())

        return ret

    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Finvasia limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            buyOrSell = 'B' if order.isBuy() else 'S'
            # "C" For CNC, "M" FOR NRML, "I" FOR MIS, "B" FOR BRACKET ORDER, "H" FOR COVER ORDER
            productType = 'I'
            splitStrings = order.getInstrument().split('|')
            exchange = splitStrings[0] if len(splitStrings) > 1 else 'NSE'
            symbol = splitStrings[1] if len(
                splitStrings) > 1 else order.getInstrument()
            quantity = order.getQuantity()
            price = order.getLimitPrice() if order.getType() in [
                broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT] else 0
            stopPrice = order.getStopPrice() if order.getType() in [
                broker.Order.Type.STOP_LIMIT] else 0
            priceType = {
                # LMT / MKT / SL-LMT / SL-MKT / DS / 2L / 3L
                broker.Order.Type.MARKET: 'MKT',
                broker.Order.Type.LIMIT: 'LMT',
                broker.Order.Type.STOP_LIMIT: 'SL-LMT',
                broker.Order.Type.STOP: 'SL-MKT'
            }.get(order.getType())
            retention = 'DAY'  # DAY / EOS / IOC

            logger.info(
                f'Placing {priceType} {"Buy" if order.isBuy() else "Sell"} order for {order.getInstrument()} with {quantity} quantity')
            try:
                finvasiaOrder = self.__placeOrder(buyOrSell,
                                                  productType,
                                                  exchange,
                                                  symbol,
                                                  quantity,
                                                  price,
                                                  priceType,
                                                  stopPrice,
                                                  retention,
                                                  None)
            except Exception as e:
                logger.critical(f'Could not place order for {symbol}. Reason: {e}')
                return

            logger.info(
                f'Placed {priceType} {"Buy" if order.isBuy() else "Sell"} order {finvasiaOrder.getId()} at {finvasiaOrder.getDateTime()}')
            order.setSubmitted(finvasiaOrder.getId(),
                               finvasiaOrder.getDateTime())
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

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self.__api.cancel_order(orderno=order.getId())
        self._unregisterOrder(order)
        order.switchState(broker.Order.State.CANCELED)

        # Update cash and shares.
        self.refreshAccountBalance()

        # Notify that the order was canceled.
        self.notifyOrderEvent(broker.OrderEvent(
            order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))

    # END broker.Broker interface
