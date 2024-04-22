import logging
import datetime
import math

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State, Expiry
from pyalgomate.cli import CliMain


class RollingStraddleIntraday(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, telegramBot=None):
        super(RollingStraddleIntraday, self).__init__(feed, broker,
                                                      strategyName=strategyName if strategyName else __class__.__name__,
                                                      logger=logging.getLogger(
                                                          __file__),
                                                      telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=25)
        self.expiry = Expiry.WEEKLY

        self.underlying = underlying
        underlyingDetails = self.getBroker().getUnderlyingDetails(self.underlying)
        self.underlyingIndex = underlyingDetails['index']
        self.strikeDifference = underlyingDetails['strikeDifference']
        self.lotSize = underlyingDetails['lotSize']

        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 4000
        self.strikeThreshold = self.strikeDifference + 20

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.lastAtmStrike = None
        self.numberOfAdjustments = 0

    def closeAllPositions(self):
        self.state = State.EXITED
        for position in self.getActivePositions().copy():
            if position.getEntryOrder().isFilled():
                position.exitMarket()

    def getATMStrike(self):
        underlyingLTP = self.getLastPrice(self.underlying)

        if underlyingLTP is None:
            return None

        inputPrice = int(underlyingLTP)
        remainder = int(inputPrice % self.strikeDifference)
        if remainder < int(self.strikeDifference / 2):
            return inputPrice - remainder
        else:
            return inputPrice + (self.strikeDifference - remainder)

    def takePositions(self, currentExpiry):
        atmStrike = self.getATMStrike()

        if atmStrike is None:
            return

        ceSymbol = self.getBroker().getOptionSymbol(
            self.underlying, currentExpiry, atmStrike, 'C')
        peSymbol = self.getBroker().getOptionSymbol(
            self.underlying, currentExpiry, atmStrike, 'P')

        if not (self.haveLTP(ceSymbol) and self.haveLTP(peSymbol)):
            return

        self.state = State.PLACING_ORDERS

        self.log(f"Taking positions at strike {atmStrike}")
        pendingSymbols = [ceSymbol, peSymbol]
        # Place initial positions
        if self.lastAtmStrike is not None:
            pendingSymbols = [peSymbol, ceSymbol] if atmStrike > self.lastAtmStrike else [
                ceSymbol, peSymbol]

        for symbol in pendingSymbols:
            if symbol not in [position.getInstrument() for position in self.getActivePositions()]:
                self.enterShort(
                    symbol, self.quantity)

        self.lastAtmStrike = atmStrike

    def canExitAllPositions(self, currentTime):
        # Exit all positions if exit time is met or portfolio SL is hit
        if currentTime >= self.exitTime:
            self.log(
                f"Current time {currentTime} is >= Exit time {self.exitTime}. Closing all positions!")
            return True

        self.overallPnL = self.getOverallPnL()

        if self.overallPnL <= -self.portfolioSL:
            self.log(
                f"Portfolio SL({self.portfolioSL} is hit. Current PnL is {self.overallPnL}. Exiting all positions!)")
            return True

        return False

    def canDoAdjustments(self):
        if self.lastAtmStrike is None:
            return False

        underlyingPrice = self.getLastPrice(self.underlying)

        if not ((self.lastAtmStrike - self.strikeThreshold) < underlyingPrice < (
                self.lastAtmStrike + self.strikeThreshold)):
            atmStrike = self.getATMStrike()
            self.log(
                f"Underlying price {underlyingPrice} has exceeded strike threshold {self.strikeThreshold}. Current positions are at strike {atmStrike}")
            return True

        return False

    def doAdjustments(self, expiry):
        if self.lastAtmStrike is None:
            return

        atmStrike = self.getATMStrike()
        self.state = State.PLACING_ORDERS
        for position in list(self.getActivePositions()):
            if not position.getEntryOrder().isFilled():
                continue
            optionContract = self.getBroker().getOptionContract(position.getInstrument())
            if optionContract.strike == self.lastAtmStrike:
                if atmStrike > self.lastAtmStrike:
                    if optionContract.type == 'c':
                        position.exitMarket()
                        break
                else:
                    if optionContract.type == 'p':
                        position.exitMarket()
                        break

        # open a new straddle
        self.takePositions(expiry)

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date(
        ), self.underlyingIndex) if self.expiry == Expiry.WEEKLY else utils.getNearestMonthlyExpiryDate(
            bars.getDateTime().date(), self.underlyingIndex)

        if self.state == State.LIVE:
            if bars.getDateTime().time() >= self.entryTime and bars.getDateTime().time() < self.exitTime:
                self.takePositions(currentExpiry)
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif self.state == State.ENTERED:
            if self.canExitAllPositions(bars.getDateTime().time()):
                self.closeAllPositions()
            elif self.canDoAdjustments():
                self.doAdjustments(currentExpiry)
                self.numberOfAdjustments += 1
        # Check if we are in the EXITED state
        elif self.state == State.EXITED:
            pass

        self.overallPnL = self.getOverallPnL()

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()


if __name__ == "__main__":
    CliMain(RollingStraddleIntraday)
