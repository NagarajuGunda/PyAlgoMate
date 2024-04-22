from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.core import State
from pyalgomate.cli import CliMain
import logging
import pyalgotrade.bar

'''
After 2 PM, go in the direction of break of day high or low with stop of breakout candle high or low
'''


class BreakoutV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, registeredOptionsCount=0, strategyName=None, callback=None,
                 resampleFrequency=None, lotSize=None, collectData=None, telegramBot=None):
        super(BreakoutV1, self).__init__(feed, broker,
                                         strategyName=strategyName if strategyName else __class__.__name__,
                                         logger=logging.getLogger(
                                             __file__),
                                         callback=callback,
                                         collectData=collectData,
                                         telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=14, minute=0)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.underlying = underlying
        self.strikeDifference = 100
        self.registeredOptionsCount = registeredOptionsCount
        self.callback = callback
        self.__reset__()

        self.resampleBarFeed(
            5 * pyalgotrade.bar.Frequency.MINUTE, self.on5MinBars)

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.position = None
        self.dayHigh = None
        self.dayLow = None
        self.positionSL = None

    def on5MinBars(self, bars):
        if not ((self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime)):
            return

        bar = bars.getBar(self.underlying)

        if bar is None:
            return

        currentExpiry = utils.getNearestWeeklyExpiryDate(
            bars.getDateTime().date())

        if bar.getClose() > self.dayHigh:
            atmStrike = self.getATMStrike(
                bar.getClose(), self.strikeDifference)
            symbol = self.getOptionSymbol(
                self.underlying, currentExpiry, atmStrike, 'c')
            if bars.getBar(symbol) is None:
                return

            self.log(
                f'<{self.underlying}> close <{bar.getClose()}> is higher than day high <{self.dayHigh}>. Entering bullish position')
            self.state = State.PLACING_ORDERS
            self.position = self.enterLong(symbol, self.quantity)
            atmCEBar = bars.getBar(symbol)
            self.log(
                f'Entering <{symbol}> long at <{atmCEBar.getClose()}> with SL <{atmCEBar.getLow()}>')
            self.positionSL = atmCEBar.getLow()
        elif bar.getClose() < self.dayLow:
            atmStrike = self.getATMStrike(
                bar.getClose(), self.strikeDifference)
            symbol = self.getOptionSymbol(
                self.underlying, currentExpiry, atmStrike, 'p')
            if bars.getBar(symbol) is None:
                return

            self.log(
                f'<{self.underlying}> close <{bar.getClose()}> is lower than day low <{self.dayLow}>. Entering bearish position')
            self.state = State.PLACING_ORDERS
            self.position = self.enterLong(symbol, self.quantity)
            atmPEBar = bars.getBar(symbol)
            self.log(
                f'Entering <{symbol}> long at <{atmPEBar.getClose()}> with SL <{atmPEBar.getLow()}>')
            self.positionSL = atmPEBar.getLow()

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        self.overallPnL = self.getOverallPnL()

        bar = bars.getBar(self.underlying)

        if bar is None:
            return

        if bars.getDateTime().time() < self.entryTime:
            self.dayHigh = bar.getHigh() if ((self.dayHigh is None) or (
                bar.getHigh() > self.dayHigh)) else self.dayHigh
            self.dayLow = bar.getLow() if ((self.dayLow is None) or (
                bar.getLow() < self.dayLow)) else self.dayLow

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        elif bars.getDateTime().time() >= self.exitTime:
            if (self.state != State.EXITED) and (len(self.getActivePositions()) > 0):
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time <{self.exitTime}. Closing all positions!')
                for position in list(self.getActivePositions()):
                    if not position.exitActive():
                        position.exitMarket()
                self.position = None
                self.state = State.EXITED
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif self.state == State.ENTERED:
            ltp = self.getLTP(self.position.getInstrument())
            if ltp < self.positionSL:
                self.log(
                    f'<{self.position.getInstrument()}> LTP <{ltp}> has crossed SL <{self.positionSL}>. Exiting position')
                self.state = State.EXITED
                self.position.exitMarket()


if __name__ == "__main__":
    CliMain(BreakoutV1)
