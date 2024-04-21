from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.core import State
from pyalgomate.cli import CliMain
import logging
import pyalgotrade.bar

'''
Ratio backspread based on CPR
'''


class CPRV2(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, callback=None, lotSize=None, collectData=None, telegramBot=None):
        super(CPRV2, self).__init__(feed, broker,
                                    strategyName=strategyName if strategyName else __class__.__name__,
                                    logger=logging.getLogger(
                                        __file__),
                                    callback=callback,
                                    collectData=collectData,
                                    telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=9, minute=20)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 2000
        self.underlying = underlying
        self.callback = callback
        self.strikeDifference = 100
        self.__reset__()

        self.resampleBarFeed(
            5 * pyalgotrade.bar.Frequency.MINUTE, self.on5MinBars)

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionBullish = self.positionBearish = []
        self.centralPivot = None
        self.bottomPivot = None
        self.topPivot = None
        self.dayHigh = None
        self.dayLow = None

    def on5MinBars(self, bars):
        underlyingBar = bars.getBar(self.underlying)

        currentExpiry = utils.getNearestWeeklyExpiryDate(
            bars.getDateTime().date())

        if (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime) and (self.centralPivot is not None):
            if bars.getDateTime().time() == self.entryTime:
                if underlyingBar.getClose() > self.topPivot:
                    # Bullish entry
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), self.strikeDifference)
                    atmSymbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'c')
                    atmLTP = self.getLTP(atmSymbol)

                    if atmLTP is None:
                        self.log(f'{atmSymbol} LTP is not found', logging.WARN)
                        return

                    otmGreeks = self.getNearestPremiumOption(
                        'c', atmLTP / 2, currentExpiry)

                    if otmGreeks is None:
                        self.log(
                            f'Nearest <{atmLTP / 2}> premium CE option is not found', logging.WARN)
                        return

                    self.log(
                        f'Found a bullish setup. Initiating a Ratro Backspread')
                    self.state = State.PLACING_ORDERS
                    self.positionBearish.append(self.enterLong(
                        otmGreeks.optionContract.symbol, 2 * self.quantity))
                    self.positionBearish.append(
                        self.enterShort(atmSymbol, self.quantity))
                elif underlyingBar.getClose() < self.bottomPivot:
                    # Bearish entry
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), self.strikeDifference)
                    atmSymbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'p')
                    atmLTP = self.getLTP(atmSymbol)

                    if atmLTP is None:
                        self.log(f'{atmSymbol} LTP is not found', logging.WARN)
                        return

                    otmGreeks = self.getNearestPremiumOption(
                        'p', atmLTP / 2, currentExpiry)

                    if otmGreeks is None:
                        self.log(
                            f'Nearest <{atmLTP / 2}> premium PE option is not found', logging.WARN)
                        return

                    self.log(
                        f'Found a bearish setup. Initiating a Ratro Backspread')
                    self.state = State.PLACING_ORDERS
                    self.positionBearish.append(self.enterLong(
                        otmGreeks.optionContract.symbol, 2 * self.quantity))
                    self.positionBearish.append(
                        self.enterShort(atmSymbol, self.quantity))

                else:
                    pass
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        optionData = self.getOptionData(bars)

        bar = bars.getBar(self.underlying)

        if bar is None:
            return

        self.overallPnL = self.getOverallPnL()

        self.dayHigh = bar.getHigh() if ((self.dayHigh is None) or (
            bar.getHigh() > self.dayHigh)) else self.dayHigh
        self.dayLow = bar.getLow() if ((self.dayLow is None) or (
            bar.getLow() < self.dayLow)) else self.dayLow

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")

            centralPivot = (self.dayHigh + self.dayLow +
                            self.getLTP(self.underlying)) / 3
            tempBottomPivot = (self.dayHigh + self.dayLow) / 2
            tempTopPivot = (centralPivot - tempBottomPivot) + centralPivot

            bottomPivot = tempTopPivot if (
                tempBottomPivot > tempTopPivot) else tempBottomPivot
            topPivot = tempTopPivot if (
                bottomPivot == tempBottomPivot) else tempBottomPivot

            if self.state != State.LIVE:
                self.__reset__()

            self.dayHigh = None
            self.dayLow = None
            self.centralPivot = centralPivot
            self.topPivot = topPivot
            self.bottomPivot = bottomPivot

            self.log(
                f'Pivots for the next day are - Central Pivot <{self.centralPivot}> Top Pivot <{self.topPivot}> Bottom Pivot <{self.bottomPivot}>')
        elif (bars.getDateTime().time() >= self.exitTime):
            if (self.state != State.EXITED) and (len(self.openPositions) > 0):
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time <{self.exitTime}. Closing all positions!')
                for position in list(self.getActivePositions()):
                    if not position.exitActive():
                        position.exitMarket()
                self.positionBullish = self.positionBearish = []
                self.state = State.EXITED


if __name__ == "__main__":
    CliMain(CPRV2)
