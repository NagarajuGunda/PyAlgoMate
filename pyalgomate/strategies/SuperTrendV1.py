import logging
import datetime
from talipp.indicators import SuperTrend
from talipp.indicators.SuperTrend import SuperTrendVal, Trend
from talipp.ohlcv import OHLCV

import pyalgotrade.bar as bar

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State
from pyalgomate.core import resampled

class SuperTrendV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None,
                 callback=None, collectData=None, telegramBot=None):
        super(SuperTrendV1, self).__init__(feed, broker,
                                           strategyName=strategyName if strategyName else __class__.__name__,
                                           logger=logging.getLogger(__file__),
                                           callback=callback,
                                           collectData=collectData,
                                           telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)

        self.underlying = underlying
        underlyingDetails = self.getBroker().getUnderlyingDetails(self.underlying)
        self.underlyingIndex = underlyingDetails['index']
        self.strikeDifference = underlyingDetails['strikeDifference']
        self.lotSize = underlyingDetails['lotSize']

        self.lots = 1
        self.quantity = self.lotSize * self.lots

        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 2000
        self.underlying = underlying

        self.__reset__()
        self.supertrendLength = 7
        self.supertrendMultiplier = 3
        self.supertrend = SuperTrend(self.supertrendLength, self.supertrendMultiplier)
        self.resampleFrequency = 5

        self.resampledBars = resampled.ResampledBars(
            self.getFeed(), self.resampleFrequency * bar.Frequency.MINUTE, self.onResampledBars)

    def __reset__(self):
        super().reset()
        self.positionBullish = None
        self.positionBearish = None

    def onStart(self):
        super().onStart()

        # get historical data
        historicalData = self.getHistoricalData(self.underlying, datetime.timedelta(days=30), str(self.resampleFrequency))

        for index, row in historicalData.iterrows():
            self.addSuperTrend(row['Date/Time'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])

    def addSuperTrend(self, dateTime, open, high, low, close, volume):
        self.supertrend.add(OHLCV(open, high, low, close, volume, dateTime))

    def onResampledBars(self, bars: bar.Bars):
        if not (self.entryTime < self.getCurrentDateTime().time() < self.exitTime):
            return

        underlyingBar = bars.getBar(self.underlying)

        if underlyingBar is None:
            return

        self.addSuperTrend(underlyingBar.getDateTime(), underlyingBar.getOpen(), underlyingBar.getHigh(),
                           underlyingBar.getLow(), underlyingBar.getClose(), underlyingBar.getVolume())

        if len(self.supertrend) < 10:
            return

        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date())
        supertrendValue: SuperTrendVal = self.supertrend[-1]
        lastClose = underlyingBar.getClose()
        self.log(f'{bars.getDateTime()} - {self.underlying} - LTP <{lastClose}> Supertrend <{supertrendValue.value}>', logging.DEBUG, False)

        if supertrendValue.trend == Trend.UP:
            if self.positionBearish:
                self.log(f'{bars.getDateTime()} - Supertrend trend is UP. Exiting short position')
                self.state = State.PLACING_ORDERS
                self.positionBearish.exitMarket()
                self.positionBearish = None
            if self.positionBullish is None:
                atmStrike = self.getATMStrike(lastClose, self.strikeDifference)
                peSymbol = self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, 'p')
                self.log(f'{bars.getDateTime()} - Supertrend trend is UP. Entering PE {peSymbol} short')
                self.state = State.PLACING_ORDERS
                self.positionBullish = self.enterShort(peSymbol, self.quantity)
        elif supertrendValue.trend == Trend.DOWN:
            if self.positionBullish:
                self.log(f'{bars.getDateTime()} - Supertrend trend is DOWN. Exiting last position')
                self.state = State.PLACING_ORDERS
                self.positionBullish.exitMarket()
                self.positionBullish = None
            if self.positionBearish is None:
                atmStrike = self.getATMStrike(lastClose, self.strikeDifference)
                ceSymbol = self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, 'c')
                self.log(f'{bars.getDateTime()} - Supertrend trend is DOWN. Entering CE {ceSymbol} short')
                self.state = State.PLACING_ORDERS
                self.positionBearish = self.enterShort(ceSymbol, self.quantity)

    def onBars(self, bars: bar.Bars):
        try:
            self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

            self.overallPnL = self.getOverallPnL()

            if bars.getDateTime().time() >= self.exitTime:
                if self.state != State.EXITED and len(self.getActivePositions()) > 0:
                    self.log(f"Current time {bars.getDateTime().time()} is >= Exit time {self.exitTime}. Closing all positions!")
                    for position in list(self.getActivePositions()):
                        if not position.exitActive():
                            position.exitMarket()
                    self.positionBearish = self.positionBullish = None
                    self.state = State.EXITED

            if bars.getDateTime().time() >= self.marketEndTime:
                if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                    self.log(
                        f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
                if self.state != State.LIVE:
                    self.__reset__()
        except Exception as e:
            pass
        finally:
            self.resampledBars.addBars(bars.getDateTime(), bars)


if __name__ == "__main__":
    from pyalgomate.cli import CliMain
    CliMain(SuperTrendV1)
