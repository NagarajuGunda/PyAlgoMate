"""
.. moduleauthor:: Nagaraju Gunda
"""

import threading
import time
import logging
import datetime
import six
import calendar

from pyalgotrade import broker
from pyalgotrade.broker import backtesting

logger = logging.getLogger(__file__)

# In a backtesting or paper-trading scenario the BacktestingBroker dispatches events while processing events from the
# BarFeed.
# It is guaranteed to process BarFeed events before the strategy because it connects to BarFeed events before the
# strategy.

class QuantityTraits(broker.InstrumentTraits):
    def roundQuantity(self, quantity):
        return round(quantity, 2)

class BacktestingBroker(backtesting.Broker):
    """A Finvasia backtesting broker.

    :param cash: The initial amount of cash.
    :type cash: int/float.
    :param barFeed: The bar feed that will provide the bars.
    :type barFeed: :class:`pyalgotrade.barfeed.BarFeed`
    :param fee: The fee percentage for each order. Defaults to 0.25%.
    :type fee: float.

    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
    """

    def findOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return underlyingInstrument + str(ceStrikePrice) + "CE", underlyingInstrument + str(peStrikePrice) + "PE"

    def __init__(self, cash, barFeed, fee=0.0025):
        commission = backtesting.TradePercentage(fee)
        super(BacktestingBroker, self).__init__(cash, barFeed, commission)

    def getInstrumentTraits(self, instrument):
        return QuantityTraits()


    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Finvasia limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)
        return super(BacktestingBroker, self).submitOrder(order)

    def _remapAction(self, action):
        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY:          broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT:   broker.Order.Action.SELL,
            broker.Order.Action.SELL:         broker.Order.Action.SELL
        }.get(action, None)
        if action is None:
            raise Exception("Only BUY/SELL orders are supported")
        return action

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
       action = self._remapAction(action)
       return super(BacktestingBroker, self).createMarketOrder(action, instrument, quantity, onClose)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        action = self._remapAction(action)

        if action == broker.Order.Action.BUY:
            # Check that there is enough cash.
            fee = self.getCommission().calculate(None, limitPrice, quantity)
            cashRequired = limitPrice * quantity + fee
            if cashRequired > self.getCash(False):
                raise Exception("Not enough cash")
        elif action == broker.Order.Action.SELL:
            # Check that there are enough coins.
            if quantity > self.getShares(instrument):
                raise Exception("Not enough %s" % (instrument))
        else:
            raise Exception("Only BUY/SELL orders are supported")

        return super(BacktestingBroker, self).createLimitOrder(action, instrument, limitPrice, quantity)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise Exception("Stop orders are not supported")

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise Exception("Stop limit orders are not supported")


class PaperTradingBroker(BacktestingBroker):
    """A Finvasia paper trading broker.
    """

    def findOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        symbol = 'NFO|NIFTY'
        if 'NIFTY BANK' in underlyingInstrument:
            symbol = 'NFO|BANKNIFTY'

        offset = (expiry.weekday() - calendar.THURSDAY) % 7
        day = expiry.day + offset
        yearMonthDay = str(expiry.year % 100) + \
            calendar.month_abbr[expiry.month].upper() + f"{day:02d}"
        return symbol + yearMonthDay + "C" + str(ceStrikePrice), symbol + yearMonthDay + "P" + str(peStrikePrice)

    pass


class TradeMonitor(threading.Thread):
    POLL_FREQUENCY = 2

    # Events
    ON_USER_TRADE = 1

    def __init__(self, httpClient):
        super(TradeMonitor, self).__init__()
        self.__lastTradeId = -1
        self.__httpClient = httpClient
        self.__queue = six.moves.queue.Queue()
        self.__stop = False

    # def _getNewTrades(self):
    #     userTrades = self.__httpClient.getUserTransactions(
    #         httpclient.HTTPClient.UserTransactionType.MARKET_TRADE)

    #     # Get the new trades only.
    #     ret = [t for t in userTrades if t.getId() > self.__lastTradeId]

    #     # Sort by id, so older trades first.
    #     return sorted(ret, key=lambda t: t.getId())

    def getQueue(self):
        return self.__queue

    def start(self):
        trades = self._getNewTrades()
        # Store the last trade id since we'll start processing new ones only.
        if len(trades):
            self.__lastTradeId = trades[-1].getId()
            logger.info("Last trade found: %d" % (self.__lastTradeId))

        super(TradeMonitor, self).start()

    def run(self):
        while not self.__stop:
            try:
                trades = self._getNewTrades()
                if len(trades):
                    self.__lastTradeId = trades[-1].getId()
                    logger.info("%d new trade/s found" % (len(trades)))
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
        return datetime.datetime.strptime(self.__dict["request_time"], "%H:%M:%S %m-%d-%Y")

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

    def findOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        symbol = 'NFO|NIFTY'
        if 'NIFTY BANK' in underlyingInstrument:
            symbol = 'NFO|BANKNIFTY'

        offset = (expiry.weekday() - calendar.THURSDAY) % 7
        day = expiry.day + offset
        yearMonthDay = str(expiry.year % 100) + \
            calendar.month_abbr[expiry.month].upper() + f"{day:02d}"
        return symbol + yearMonthDay + "C" + str(ceStrikePrice), symbol + yearMonthDay + "P" + str(peStrikePrice)

    def __init__(self, api):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__api = api
        self.__tradeMonitor = TradeMonitor(self.__httpClient)
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders = {}

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
        balance = self.__httpClient.getAccountBalance()

        # Cash
        self.__cash = round(balance.getUSDAvailable(), 2)
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
        self.__stop = True  # Stop running in case of errors.
        logger.info("Retrieving open orders.")
        openOrders = self.__httpClient.getOpenOrders()
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

    def _onUserTrades(self, trades):
        for trade in trades:
            order = self.__activeOrders.get(trade.getOrderId())
            if order is not None:
                fee = trade.getFee()
                fillPrice = trade.getBTCUSD()
                btcAmount = trade.getBTC()
                dateTime = trade.getDateTime()

                # Update cash and shares.
                self.refreshAccountBalance()
                # Update the order.
                orderExecutionInfo = broker.OrderExecutionInfo(
                    fillPrice, abs(btcAmount), fee, dateTime)
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
                logger.info("Trade %d refered to order %d that is not active" % (
                    trade.getId(), trade.getOrderId()))

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
        ret = OrderResponse(self.__api.place_order(self, buy_or_sell=buyOrSell, product_type=productType,
                                                   exchange=exchange, tradingsymbol=symbol,
                                                   quantity=quantity, discloseqty=0, price_type=priceType,
                                                   price=price, trigger_price=triggerPrice,
                                                   retention=retention, remarks=remarks))

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
            productType = 'C'
            exchange = 'NSE'
            symbol = order.getInstrument()
            quantity = order.getQuantity()
            price = order.getLimitPrice()
            stopPrice = order.getStopPrice()
            priceType = {
                # LMT / MKT / SL-LMT / SL-MKT / DS / 2L / 3L
                broker.Order.Type.MARKET: 'MKT',
                broker.Order.Type.LIMIT: 'LMT',
                broker.Order.Type.STOP_LIMIT: 'SL-LMT',
                broker.Order.Type.STOP: 'SL-MKT'
            }.get(order.getType())
            retention = 'DAY'  # DAY / EOS / IOC

            finvasiaOrder = self.__placeOrder(buyOrSell,
                                              productType,
                                              exchange,
                                              symbol,
                                              quantity,
                                              price,
                                              stopPrice,
                                              priceType,
                                              retention,
                                              None)

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
            return orderType(action, instrument, quantity, False, None)
        elif orderType == broker.LimitOrder:
            return orderType(action, instrument, price, quantity, None)
        elif orderType == broker.StopOrder:
            return orderType(action, instrument, stopPrice, quantity, None)
        elif orderType == broker.StopLimitOrder:
            return orderType(action, instrument, stopPrice, price, quantity, None)

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        return self._createOrder(broker.MarketOrder, action, instrument, quantity, None, None)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return self._createOrder(broker.LimitOrder, action, instrument, quantity, limitPrice, None)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        return self._createOrder(broker.LimitOrder, action, instrument, quantity, None, stopPrice)

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        return self._createOrder(broker.LimitOrder, action, instrument, quantity, limitPrice, stopPrice)

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self.__httpClient.cancelOrder(order.getId())
        self._unregisterOrder(order)
        order.switchState(broker.Order.State.CANCELED)

        # Update cash and shares.
        self.refreshAccountBalance()

        # Notify that the order was canceled.
        self.notifyOrderEvent(broker.OrderEvent(
            order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))

    # END broker.Broker interface
