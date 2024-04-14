import logging
import datetime
from pyalgotrade.strategy import position

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State
from pyalgomate.cli import CliMain

import log_setup  # noqa
logger = logging.getLogger(__file__)

'''
Initiate a straddle after entryTime
Whenever the straddle makes a loss of more than certain amount,
Initiate a ratio backspread in the same direction
Exit at exitTime
'''


class StraddleIntradayV3(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, callback=None,
                 lotSize=None, collectData=None, telegramBot=None):
        super(StraddleIntradayV3, self).__init__(feed, broker,
                                                 strategyName=strategyName if strategyName else __class__.__name__,
                                                 logger=logging.getLogger(
                                                     __file__),
                                                 callback=callback,
                                                 collectData=collectData,
                                                 telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=10, minute=5)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.marketEndTime = datetime.time(hour=15, minute=30)
        self.underlying = underlying
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.strikeDifference = 100
        self.portfolioSL = 2000
        self.adjustmentSL = 1000

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.straddleStrike = None
        self.adjustsmentsDone = False

    def _enterShortStraddle(self, currentDate):
        underlyingLTP = self.getLTP(self.underlying)
        if underlyingLTP is None:
            return None

        atmStrike = self.getATMStrike(underlyingLTP, self.strikeDifference)

        currentExpiry = utils.getNearestWeeklyExpiryDate(
            currentDate)

        ceSymbol = self.getOptionSymbol(
            self.underlying, currentExpiry, atmStrike, 'c')
        peSymbol = self.getOptionSymbol(
            self.underlying, currentExpiry, atmStrike, 'p')

        if (ceSymbol is not None and self.haveLTP(ceSymbol) is None) or (peSymbol is not None and self.haveLTP(peSymbol) is None):
            return None

        self.state = State.PLACING_ORDERS
        self.enterShort(ceSymbol, self.quantity)
        self.enterShort(peSymbol, self.quantity)
        self.straddleStrike = atmStrike
        self.log(
            f'Taking a straddle position with {ceSymbol} and {peSymbol}')
        self.log(
            f'Underlying LTP is <{underlyingLTP}>, <{ceSymbol}> LTP is <{self.getLTP(ceSymbol)}> and <{peSymbol}> LTP is <{self.getLTP(peSymbol)}>')

    def closeAllPositions(self):
        if self.state == State.EXITED:
            return

        self.state = State.EXITED
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()

    def doAdjustments(self, currentDate):
        # Take a ratio backspread
        underlyingLTP = self.getLTP(self.underlying)
        if underlyingLTP is None:
            return None

        atmStrike = self.getATMStrike(underlyingLTP, self.strikeDifference)

        currentExpiry = utils.getNearestWeeklyExpiryDate(
            currentDate)

        # Cut the loss making leg
        exitSymbol = self.getOptionSymbol(self.underlying, currentExpiry, self.straddleStrike, 'p' if atmStrike < self.straddleStrike else 'c')
        exitPosition = None
        for openPosition in list(self.getActivePositions()):
            if openPosition.getInstrument() == exitSymbol:
                exitPosition = openPosition
                break

        itmOtmStrikeDifference = 1 * self.strikeDifference
        itmOtmQuantityRatio = 1
        itmSymbol = self.getOptionSymbol(
            self.underlying, currentExpiry, (atmStrike + self.strikeDifference) if atmStrike < self.straddleStrike else (atmStrike - self.strikeDifference), 'p' if atmStrike < self.straddleStrike else 'c')
        otmSymbol = self.getOptionSymbol(
            self.underlying, currentExpiry, (atmStrike - itmOtmStrikeDifference) if atmStrike < self.straddleStrike else (atmStrike + itmOtmStrikeDifference), 'p' if atmStrike < self.straddleStrike else 'c')

        if self.haveLTP(itmSymbol) is None or self.haveLTP(otmSymbol) is None:
            return None

        self.state = State.PLACING_ORDERS
        exitPosition.exitMarket()
        self.enterLong(otmSymbol, itmOtmQuantityRatio * self.quantity)
        self.enterShort(itmSymbol, self.quantity)
        self.adjustsmentsDone = True
        self.log(f'Cutting the loss making position <{exitSymbol}> at <{self.getLTP(exitSymbol)}>')
        self.log(
            f'Taking a ratio backspread with ITM {itmSymbol} and OTM {otmSymbol}')
        self.log(
            f'Underlying LTP is <{underlyingLTP}>, <{itmSymbol}> LTP is <{self.getLTP(itmSymbol)}> and <{otmSymbol}> LTP is <{self.getLTP(otmSymbol)}>')

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        self.overallPnL = self.getOverallPnL()

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        # Exit all positions if exit time is met or portfolio SL is hit
        elif bars.getDateTime().time() >= self.exitTime:
            if self.state != State.EXITED:
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time <{self.exitTime}. Closing all positions!')
                self.closeAllPositions()
        elif self.overallPnL <= -self.portfolioSL:
            if self.state != State.EXITED:
                self.log(
                    f'Current PnL <{self.overallPnL}> has crossed potfolio SL <{self.portfolioSL}>. Closing all positions!')
                self.closeAllPositions()
        elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            self.log(
                f'Entry time <{self.entryTime}> is greater than current time<{bars.getDateTime()}>.')
            self._enterShortStraddle(bars.getDateTime().date())
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif self.state == State.ENTERED:
            if (self.adjustsmentsDone is False) and (self.overallPnL <= -self.adjustmentSL):
                self.log(
                    f'Current PnL <{self.overallPnL}> has crossed the adjustments SL <{self.adjustmentSL}>. Doing adjustments!')
                self.doAdjustments(bars.getDateTime().date())
        elif self.state == State.EXITED:
            pass


if __name__ == "__main__":
    CliMain(StraddleIntradayV3)
