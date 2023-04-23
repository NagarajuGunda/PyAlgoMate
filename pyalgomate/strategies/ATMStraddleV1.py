from pyalgotrade.barfeed.resampled import BarsGrouper
from pyalgotrade import bar
from pyalgotrade import resamplebase
import logging
import datetime
import calendar
from talipp.indicators import SuperTrend
from talipp.indicators.SuperTrend import Trend
from talipp.ohlcv import OHLCV
import pandas as pd

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State

logger = logging.getLogger(__file__)


class ResampledBars():
    def __init__(self, frequency, strategy, callback):

        self.__values = []
        self.__grouper = None
        self.__range = None
        self.__frequency = frequency
        self.__strategy = strategy
        self.__callback = callback

        self.__strategy.getFeed().getNewValuesEvent().subscribe(self.newBars)

    def getATMStraddleBar(self, dateTime):
        currentExpiry = utils.getNearestWeeklyExpiryDate(
            dateTime.date())

        atmStrike = self.__strategy.getATMStrike(
            self.__strategy.getLTP(self.__strategy.getUnderlying()), 100)

        ceSymbol = self.__strategy.getOptionSymbol(
            self.__strategy.getUnderlying(), currentExpiry, atmStrike, 'c')
        peSymbol = self.__strategy.getOptionSymbol(
            self.__strategy.getUnderlying(), currentExpiry, atmStrike, 'p')

        ceBar = self.__strategy.getFeed().getLastBar(ceSymbol)
        peBar = self.__strategy.getFeed().getLastBar(ceSymbol)

        if ceBar is None or peBar is None:
            return None

        dayMonthYear = f"{currentExpiry.day:02d}" + \
            calendar.month_abbr[currentExpiry.month].upper(
            ) + str(currentExpiry.year % 100)

        rollingStraddleTicker = f'{self.__strategy.getUnderlying()}{dayMonthYear}ATMSTRADDLE'

        return bar.BasicBar(dateTime,
                            ceBar.getOpen() + peBar.getOpen(),
                            ceBar.getHigh() + peBar.getHigh(),
                            ceBar.getLow() + peBar.getLow(),
                            ceBar.getClose() + peBar.getClose(),
                            ceBar.getVolume() + peBar.getVolume(),
                            None,
                            bar.Frequency.TRADE,
                            {
                                "Instrument": rollingStraddleTicker,
                                "Open Interest": 0,
                                "Date/Time": None
                            })

    def newBars(self, dateTime, bars):
        underlyingBar = bars.getBar(self.__strategy.getUnderlying())

        if underlyingBar is None:
            return

        barDict = {
            self.__strategy.getUnderlying(): underlyingBar}

        combinedBar = self.getATMStraddleBar(dateTime)

        if combinedBar is not None:
            barDict[combinedBar.getExtraColumns()['Instrument']] = combinedBar

        bars = bar.Bars(barDict)

        if self.__range is None:
            self.__range = resamplebase.build_range(dateTime, self.__frequency)
            self.__grouper = BarsGrouper(
                self.__range.getBeginning(), bars, self.__frequency)
        elif self.__range.belongs(dateTime):
            self.__grouper.addValue(bars)
        else:
            self.__values.append(self.__grouper.getGrouped())
            self.__range = resamplebase.build_range(dateTime, self.__frequency)
            self.__grouper = BarsGrouper(
                self.__range.getBeginning(), bars, self.__frequency)

        self.checkNow(dateTime)

    def checkNow(self, dateTime):
        if self.__range is not None and not self.__range.belongs(dateTime):
            self.__values.append(self.__grouper.getGrouped())
            self.__grouper = None
            self.__range = None

        if len(self.__values):
            self.__callback(self.__values.pop(0))


class ATMStraddleV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, callback=None, lotSize=None, collectData=None):
        super(ATMStraddleV1, self).__init__(feed, broker,
                                            strategyName=__class__.__name__,
                                            logger=logging.getLogger(
                                                __file__),
                                            callback=callback,
                                            collectData=collectData)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 2000
        self.underlying = 'BANKNIFTY'

        self.__reset__()

        self.dataColumns = ["Ticker", "Date/Time", "Open", "High",
                            "Low", "Close", "Volume", "Open Interest"]
        self.tickDf = pd.DataFrame(columns=self.dataColumns)
        self.oneMinDf = pd.DataFrame(columns=self.dataColumns)
        self.resampledDict = dict()
        self.resampleFrequency = '1T'
        self.supertrend = dict()
        self.supertrendLength = 7
        self.supertrendMultiplier = 3
        self.indicatorValuesToBeAvailable = 45

        self.resampledBars = ResampledBars(
            bar.Frequency.MINUTE, self, self.onResampledBars)

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionBullish = None
        self.positionBearish = None

    def setUnderlying(self, underlying):
        self.underlying = underlying

    def getUnderlying(self):
        return self.underlying

    def onResampledBars(self, bars):
        currentExpiry = utils.getNearestWeeklyExpiryDate(
            bars.getDateTime().date())
        bar = bars.getBar(self.underlying)

        if bar is None:
            return

        if self.underlying not in self.resampledDict:
            self.resampledDict[self.underlying] = {
                'Date/Time': [],
                'Open': [],
                'High': [],
                'Low': [],
                'Close': [],
                'Volume': [],
                'Open Interest': []
            }

        self.resampledDict[self.underlying]['Date/Time'].append(
            bar.getDateTime())
        self.resampledDict[self.underlying]['Open'].append(bar.getOpen())
        self.resampledDict[self.underlying]['High'].append(bar.getHigh())
        self.resampledDict[self.underlying]['Low'].append(bar.getLow())
        self.resampledDict[self.underlying]['Close'].append(bar.getClose())
        self.resampledDict[self.underlying]['Volume'].append(bar.getVolume())
        self.resampledDict[self.underlying]['Open Interest'].append(
            bar.getExtraColumns().get("Open Interest", 0))

        if self.underlying not in self.supertrend:
            self.supertrend[self.underlying] = SuperTrend(
                self.supertrendLength, self.supertrendMultiplier)

        ohlcv = OHLCV(bar.getOpen(), bar.getHigh(), bar.getLow(),
                      bar.getClose(), bar.getVolume(), bar.getDateTime())

        self.supertrend[self.underlying].add_input_value(ohlcv)

        if self.supertrend[self.underlying] is not None and len(self.supertrend[self.underlying]) > self.indicatorValuesToBeAvailable:
            supertrendValue = self.supertrend[self.underlying][-1]
            lastClose = self.resampledDict[self.underlying]['Close'][-1]
            self.log(
                f'{bars.getDateTime()} - {self.underlying} - LTP <{lastClose}> Supertrend <{supertrendValue.value}>', logging.DEBUG)

            # Green
            if supertrendValue.trend == Trend.UP:
                if self.positionBearish is not None:
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend is UP. Exiting last position')
                    self.positionBearish.exitMarket()
                    self.positionBearish = None
                if self.positionBullish is None:
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), 100)
                    peSymbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'p')
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend is UP. Entering PE {peSymbol} short')
                    self.positionBullish = self.enterShort(
                        peSymbol, self.quantity)
            elif supertrendValue.trend == Trend.DOWN:
                if self.positionBullish is not None:
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend is DOWN. Exiting last position')
                    self.positionBullish.exitMarket()
                    self.positionBullish = None
                if self.positionBearish is None:
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), 100)
                    ceSymbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'c')
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend is DOWN. Entering CE {ceSymbol} short')
                    self.positionBearish = self.enterShort(
                        ceSymbol, self.quantity)

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        self.overallPnL = self.getOverallPnL()

        if bars.getDateTime().time() >= self.exitTime:
            if self.state != State.EXITED:
                self.log(
                    f"Current time {bars.getDateTime().time()} is >= Exit time {self.exitTime}. Closing all positions!")
                for position in list(self.getActivePositions()):
                    if not position.exitActive():
                        position.exitMarket()
                self.positionBearish = self.positionBullish = None
                self.state = State.EXITED

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.openPositions) + len(self.closedPositions)) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()


if __name__ == "__main__":
    from pyalgomate.backtesting import CustomCSVFeed
    from pyalgomate.brokers import BacktestingBroker

    underlyingInstrument = 'BANKNIFTY'

    start = datetime.datetime.now()
    feed = CustomCSVFeed.CustomCSVFeed()
    feed.addBarsFromParquets(dataFiles=[
                             "pyalgomate/backtesting/data/test.parquet"], ticker=underlyingInstrument)

    print("")
    print(f"Time took in loading data <{datetime.datetime.now()-start}>")
    start = datetime.datetime.now()

    broker = BacktestingBroker(200000, feed)
    strat = ATMStraddleV1(feed=feed, broker=broker, lotSize=25)
    strat.run()

    print("")
    print(
        f"Time took in running the strategy <{datetime.datetime.now()-start}>")
