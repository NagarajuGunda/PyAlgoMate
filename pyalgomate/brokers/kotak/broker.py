"""
.. moduleauthor:: Sai Krishna
"""

import csv
import calendar
import threading
import time
import logging
import datetime
import six
import re
from pyalgotrade import broker
from pyalgomate.brokers import BacktestingBroker, QuantityTraits
from pyalgomate.strategies import OptionContract
import pyalgomate.utils as utils
from neo_api_client import NeoAPI
from pyalgomate.utils import UnderlyingIndex

logger = logging.getLogger(__file__)
logger.propagate = False

underlyingMapping = {
    'MIDCPNIFTY': {
        'optionPrefix': 'MIDCPNIFTY',
        'index': UnderlyingIndex.MIDCPNIFTY,
        'lotSize': 75,
        'strikeDifference': 25
    },
    'BANKNIFTY': {
        'optionPrefix': 'BANKNIFTY',
        'index': UnderlyingIndex.BANKNIFTY,
        'lotSize': 15,
        'strikeDifference': 100
    },
    'NIFTY': {
        'optionPrefix': 'NIFTY',
        'index': UnderlyingIndex.NIFTY,
        'lotSize': 25,
        'strikeDifference': 50
    },
    'FINNIFTY': {
        'optionPrefix': 'FINNIFTY',
        'index': UnderlyingIndex.FINNIFTY,
        'lotSize': 40,
        'strikeDifference': 50
    },
    'SENSEX': {
        'optionPrefix': 'SENSEX',
        'index': UnderlyingIndex.SENSEX,
        'lotSize': 10,
        'strikeDifference': 100
    }
}


def getUnderlyingDetails(underlying):
    return underlyingMapping[underlying]


status_mapping = {
    "rejected": "REJECTED",
    "cancelled": "CANCELED",
    "complete": "COMPLETE",
    "traded": "COMPLETE"}


# def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
#     # Check if it's the last week of the month
#     expiry_month = expiry.month
#     expiry_year = expiry.year
#     last_day = calendar.monthrange(expiry_year, expiry_month)[1]
#     last_week_start = last_day - 6
#     if expiry.day >= last_week_start:
#         month_name = calendar.month_abbr[expiry_month]
#         return f"{underlyingInstrument}23{month_name.upper()}{strikePrice}{callOrPut}E"

#     expiry_month_str = str(expiry_month)
#     if expiry_month_str.startswith('0'):
#         expiry_month_str = expiry_month_str[1:]
#     expiry_day_str = str(expiry.day)
#     if expiry.day >= 1 and expiry.day <= 9:
#         expiry_day_str = f"0{expiry.day}"
#     return f"{underlyingInstrument}23{expiry_month_str}{expiry_day_str}{strikePrice}{callOrPut}E"

def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
    monthly = utils.getNearestMonthlyExpiryDate(expiry) == expiry
    strikePlusOption = str(strikePrice) + ('CE' if (callOrPut ==
                                                    'C' or callOrPut == 'Call') else 'PE')
    if monthly:
        return underlyingInstrument + str(expiry.year % 100) + calendar.month_abbr[expiry.month].upper() + strikePlusOption
    else:
        if expiry.month == 10:
            monthlySymbol = 'O'
        elif expiry.month == 11:
            monthlySymbol = 'N'
        elif expiry.month == 12:
            monthlySymbol = 'D'
        else:
            monthlySymbol = f'{expiry.month}'
        return underlyingInstrument + str(expiry.year % 100) + f"{monthlySymbol}{expiry.day:02d}" + strikePlusOption


def getOptionSymbols(underlyingInstrument, expiry, ltp, count, strikeDifference=100):
    ltp = int(float(ltp) / strikeDifference) * strikeDifference
    logger.info(
        f"Nearest strike price of {underlyingInstrument} is <{ltp}>")
    optionSymbols = []
    for n in range(-count, count+1):
        optionSymbols.append(getOptionSymbol(
            underlyingInstrument, expiry, ltp + (n * strikeDifference), 'C'))

    for n in range(-count, count+1):
        optionSymbols.append(getOptionSymbol(
            underlyingInstrument, expiry, ltp - (n * strikeDifference), 'P'))

    logger.info("Options symbols are " + ",".join(optionSymbols))
    return optionSymbols


def search_symbols_in_csv(symbols):
    filename = 'bse_fo.csv'  # Local filename

    x = symbols
    column_name = 'pScripRefKey'
    symbol_column_name = 'pSymbol'
    output_array = []

    # Perform search operation for each value in x using the local CSV file
    with open(filename, 'r') as file:
        csv_reader = csv.DictReader(file)
        for value in x:
            found_symbol = None
            for row in csv_reader:
                if row[column_name] == value:
                    found_symbol = row[symbol_column_name]
                    break
            output_array.append({"instrument_token": found_symbol,
                                "exchange_segment": "bse_fo", "instrument": value})
            # Reset the file reader to the beginning for the next search
            file.seek(0)
    return output_array


def getTokenMappings(api, underlying, symbols):
    tokenMappings = []
    if underlying == "SENSEX":
        instrument_tokens = search_symbols_in_csv(symbols)
        instrument_tokens.append(
            {"instrument_token": "1", "exchange_segment": "bse_cm", "instrument": "SENSEX"})
        return instrument_tokens

    else:
        ret = api.search_scrip('NSE' if underlying !=
                               'SENSEX' else 'BSE', underlying)
        script = [script for script in ret if script['pSymbolName']
                  == underlying][0]
        tokenMappings.append({"instrument_token": str(
            script['pSymbol']), "exchange_segment": script['pExchSeg'], "instrument": script['pTrdSymbol']})
        ret = api.search_scrip('NFO', underlying)
        for optionSymbol in ret:
            pTrdSymbol = optionSymbol['pTrdSymbol']
            pSymbol = optionSymbol['pSymbol']
            pExchSeg = optionSymbol['pExchSeg']
            if pTrdSymbol in symbols:
                tokenMappings.append(
                    {"instrument_token": pSymbol, "exchange_segment": pExchSeg, "instrument": pTrdSymbol})
        return tokenMappings


# def getHistoricalData(api, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
#     startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
#     splitStrings = exchangeSymbol.split('|')
#     exchange = splitStrings[0]

#     logger.info(
#         f'Retrieving {interval} timeframe historical data for {exchangeSymbol}')
#     ret = api.get_time_price_series(exchange=exchange, token=getKotakToken(
#         api, exchangeSymbol), starttime=startTime.timestamp(), interval=interval)
#     if ret != None:
#         df = pd.DataFrame(
#             ret)[['time', 'into', 'inth', 'intl', 'intc', 'v', 'oi']]
#         df = df.rename(columns={'time': 'Date/Time', 'into': 'Open', 'inth': 'High',
#                                 'intl': 'Low', 'intc': 'Close', 'v': 'Volume', 'oi': 'Open Interest'})
#         df['Ticker'] = exchangeSymbol
#         df[['Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']] = df[[
#             'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']].astype(float)
#         df['Date/Time'] = pd.to_datetime(df['Date/Time'],
#                                          format="%d-%m-%Y %H:%M:%S")
#         df = df[['Ticker', 'Date/Time', 'Open', 'High',
#                  'Low', 'Close', 'Volume', 'Open Interest']]
#         df = df.sort_values('Date/Time')
#         logger.info(f'Retrieved {df.shape[0]} rows of historical data')
#         return df
#     else:
#         return pd.DataFrame(columns=['Date/Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest'])


class PaperTradingBroker(BacktestingBroker):
    """A Kotak paper trading broker.
    """

    def __init__(self, cash, barFeed, fee=0.0025):
        super().__init__(cash, barFeed, fee)

        self.__api = barFeed.getApi()

    def getType(self):
        return "Paper"

    def getUnderlyingDetails(self, underlying):
        return underlyingMapping[underlying]

    # def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
    #     return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)
    def getOptionSymbol(self, underlyingInstrument, expiry: datetime.date, strikePrice, callOrPut):
        return getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, 'P')

    def getOptionContract(self, symbol):
        m = re.match(r"([A-Z\:]+)(\d{2})([A-Z]{3})(\d+)([CP])E", symbol)

        if m is not None:
            month = datetime.datetime.strptime(m.group(3), '%b').month
            year = int(m.group(2)) + 2000
            expiry = utils.getNearestMonthlyExpiryDate(
                datetime.date(year, month, 1))
            return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p", m.group(1).replace('NFO:BANKNIFTY', 'NSE:NIFTY BANK').replace('NFO:NIFTY', 'NSE:NIFTY 50'))

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
        return OptionContract(symbol, int(m.group(5)), expiry, "c" if m.group(6) == "C" else "p", m.group(1).replace('NFO:BANKNIFTY', 'NSE:NIFTY BANK').replace('NFO:NIFTY', 'NSE:NIFTY 50'))


class TradeEvent(object):
    def __init__(self, eventDict):
        self.__eventDict = eventDict

    def getId(self):
        return self.__eventDict.get('nOrdNo', None)

    def getStatus(self):
        return self.__eventDict.get('ordSt', None)

    def getRejectedReason(self):
        return self.__eventDict.get('rejRsn', None)

    def getAvgFilledPrice(self):
        return float(self.__eventDict.get('avgPrc', 0.0))

    def getTotalFilledQuantity(self):
        return float(self.__eventDict.get('fldQty', 0.0))

    def getDateTime(self):
        return datetime.datetime.strptime(self.__eventDict['flDtTm'], '%d-%b-%Y %H:%M:%S') if self.__eventDict.get('flDtTm', None) is not None else None


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
            orderHistories = self.__api.order_history(
                order_id=orderId)
            if orderHistories is None:
                logger.info(
                    f'Order history not found for order id {orderId}')
                continue

            if 'data' not in orderHistories:
                continue

            if 'data' not in orderHistories['data']:
                continue

            for orderHistory in orderHistories['data']['data']:
                status = status_mapping.get(
                    orderHistory['ordSt'], orderHistory['ordSt'])
                if status in ['put order req received', 'validation pending', 'open pending', 'OPEN', 'PENDING', 'TRIGGER_PENDING']:
                    continue

                elif orderHistory['ordSt'] in ["rejected", "cancelled", "complete", "traded"]:
                    orderHistory['ordSt'] = status
                    ret.append(TradeEvent(orderHistory))  # event
                else:
                    logger.error(
                        f'Unknown trade status {orderHistory.get("vendorCode", None)}')

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
    def __init__(self, dict):
        self.__dict = dict

    def getId(self):
        return self.__dict["nOrdNo"]

    def getDateTime(self):
        return datetime.datetime.now()

    def getStat(self):
        return self.__dict.get("stat", None)

    def getErrorMessage(self):
        return self.__dict.get("Error", None)


class LiveBroker(broker.Broker):
    """A Kotak live broker.

    :param api: Logged in api object.
    :type api: NeoApi.

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

    def getUnderlyingDetails(self, underlying):
        return underlyingMapping[underlying]

    def getOptionSymbol(self, underlyingInstrument, expiry: datetime.date, strikePrice, callOrPut):
        return getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut)

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return getOptionSymbol(underlyingInstrument, expiry, ceStrikePrice, 'C'), getOptionSymbol(underlyingInstrument, expiry, peStrikePrice, 'P')

    def getOptionContract(self, symbol):
        m = re.match(r"([A-Z\:]+)(\d{2})([A-Z]{3})(\d+)([CP])E", symbol)

        if m is not None:
            month = datetime.datetime.strptime(m.group(3), '%b').month
            year = int(m.group(2)) + 2000
            expiry = utils.getNearestMonthlyExpiryDate(
                datetime.date(year, month, 1))
            return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p", m.group(1).replace('NFO:BANKNIFTY', 'NSE:NIFTY BANK').replace('NFO:NIFTY', 'NSE:NIFTY 50'))

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
        return OptionContract(symbol, int(m.group(5)), expiry, "c" if m.group(6) == "C" else "p", m.group(1).replace('NFO:BANKNIFTY', 'NSE:NIFTY BANK').replace('NFO:NIFTY', 'NSE:NIFTY 50'))

    # def getHistoricalData(self, exchangeSymbol: str, startTime: datetime.datetime, interval: str) -> pd.DataFrame():
    #     return getHistoricalData(self.__api, exchangeSymbol, startTime, interval)

    def __init__(self, api: NeoAPI):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__api = api
        self.__tradeMonitor = TradeMonitor(self)
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders = {}

    def getType(self):
        return "Live"

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

    def __placeOrder(self, buyOrSell, productType, exchange, symbol, quantity, price, priceType, triggerPrice, retention, remarks):
        try:
            orderResponse = self.__api.place_order(exchange_segment=exchange, product=productType, price=str(price), order_type=priceType,
                                                   quantity=str(int(quantity)), validity=retention, trading_symbol=symbol,
                                                   transaction_type=buyOrSell, amo="NO", disclosed_quantity="0", market_protection="0", pf="N",
                                                   trigger_price=str(triggerPrice), tag=None)
        except Exception as e:
            raise Exception(e)

        if orderResponse is None:
            raise Exception('place_order returned None')

        ret = OrderResponse(orderResponse)

        if 'stat' not in orderResponse or orderResponse['stat'] != "Ok":
            raise Exception(ret.getErrorMessage())

        return ret

    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Kotak limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            buyOrSell = 'B' if order.isBuy() else 'S'
            # "C" For CNC, "M" FOR NRML, "I" FOR MIS, "B" FOR BRACKET ORDER, "H" FOR COVER ORDER
            productType = 'NRML'
            exchange = 'nse_fo'
            symbol = order.getInstrument()
            quantity = order.getQuantity()
            price = order.getLimitPrice() if order.getType() in [
                broker.Order.Type.LIMIT, broker.Order.Type.STOP_LIMIT] else 0
            stopPrice = order.getStopPrice() if order.getType() in [
                broker.Order.Type.STOP_LIMIT] else 0
            priceType = {
                # LMT / MKT / SL-LMT / SL-MKT / DS / 2L / 3L
                broker.Order.Type.MARKET: 'MKT',
                broker.Order.Type.LIMIT: 'L',
                broker.Order.Type.STOP_LIMIT: 'SL',
                broker.Order.Type.STOP: 'SL-M'
            }.get(order.getType())
            retention = 'DAY'  # DAY, IOC, DAY, IOC, GTC, EOS

            logger.info(
                f'Placing {priceType} {"Buy" if order.isBuy() else "Sell"} order for {order.getInstrument()} with {quantity} quantity')
            try:
                kotakOrder = self.__placeOrder(buyOrSell,
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
                logger.critical(
                    f'Could not place order for {symbol}. Reason: {e}')
                return

            try:
                logger.info(
                    f'Placed {priceType} {"Buy" if order.isBuy() else "Sell"} order {kotakOrder.getId()} at {kotakOrder.getDateTime()}')
                order.setSubmitted(kotakOrder.getId(),
                                   kotakOrder.getDateTime())
            except Exception as e:
                logger.critical(
                    f'Could not place order for {symbol}. Reason: {e}')
                return

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

        self.__api.cancel_order(order.getId())
        self._unregisterOrder(order)
        order.switchState(broker.Order.State.CANCELED)

        # Update cash and shares.
        self.refreshAccountBalance()

        # Notify that the order was canceled.
        self.notifyOrderEvent(broker.OrderEvent(
            order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))

    # END broker.Broker interface
