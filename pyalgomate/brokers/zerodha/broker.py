"""
.. moduleauthor:: Nagaraju Gunda
"""

import logging
import datetime
import calendar
import re

from pyalgomate.brokers import BacktestingBroker
from pyalgomate.strategies import OptionContract
import pyalgomate.utils as utils

logger = logging.getLogger(__file__)

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
