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

from pyalgotrade import broker
from pyalgotrade.broker import fillstrategy
from pyalgotrade.broker import backtesting
from pyalgomate.strategies import OptionContract
import pyalgomate.utils as utils
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

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

    def getOptionSymbol(self, underlyingInstrument, expiry, strikePrice, callOrPut):
        return underlyingInstrument + str(strikePrice) + ('CE' if (callOrPut == 'C' or callOrPut == 'Call') else 'PE')

    def getOptionSymbols(self, underlyingInstrument, expiry, ceStrikePrice, peStrikePrice):
        return underlyingInstrument + str(ceStrikePrice) + "CE", underlyingInstrument + str(peStrikePrice) + "PE"

    def getOptionContract(self, symbol):
        m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", symbol)

        if m is not None:
            day = int(m.group(2))
            month = m.group(3)
            year = int(m.group(4)) + 2000
            expiry = datetime.date(
                year, datetime.datetime.strptime(month, '%b').month, day)
            return OptionContract(symbol, int(m.group(6)), expiry, "c" if m.group(5) == "C" else "p", m.group(1))

        m = re.match(r"([A-Z]+)(\d+)(CE|PE)", symbol)

        if m is None:
            return None

        return OptionContract(symbol, int(m.group(2)), None, "c" if m.group(3) == "CE" else "p", m.group(1))

    def __init__(self, cash, barFeed, fee=0.0025):
        commission = backtesting.TradePercentage(fee)
        super(BacktestingBroker, self).__init__(cash, barFeed, commission)
        self.setFillStrategy(fillstrategy.DefaultStrategy(volumeLimit=None))

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


def getOptionSymbol(underlyingInstrument, expiry, strikePrice, callOrPut):
    monthly = utils.getNearestMonthlyExpiryDate(expiry) == expiry
    symbol = 'NFO:NIFTY'
    if 'NIFTY BANK' in underlyingInstrument or 'BANKNIFTY' in underlyingInstrument:
        symbol = 'NFO:BANKNIFTY'

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


class ZerodhaPaperTradingBroker(BacktestingBroker):
    """A Finvasia paper trading broker.
    """

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
            return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p", m.group(1))

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
        return OptionContract(symbol, int(m.group(5)), expiry, "c" if m.group(6) == "C" else "p", m.group(1))

    pass
