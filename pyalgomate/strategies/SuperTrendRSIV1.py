import logging
import datetime
from talipp.indicators import SuperTrend, RSI
from talipp.indicators.SuperTrend import Trend
from talipp.ohlcv import OHLCV
import pandas as pd

import pyalgotrade.bar
import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State

logger = logging.getLogger(__file__)

"""
TimeFrame: 5 minute

1. Buy conditions: Supertrend turns buy and RSI !> 85
2. Exit conditions: RSI < 55 or Supertrend turns sell
3. Sell conditions: Supertrend turns sell and RSI !< 25
4. Exit conditions: RSI > 45 or Supertrend turns buy
"""


class SuperTrendRSIV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, callback=None,
                 lotSize=None, collectData=None, telegramBot=None):
        super(SuperTrendRSIV1, self).__init__(feed, broker,
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
        self.portfolioSL = self.quantity * 1000
        self.underlying = underlying

        self.__reset__()

        self.dataColumns = ["Ticker", "Date/Time", "Open", "High",
                            "Low", "Close", "Volume", "Open Interest"]
        self.tickDf = pd.DataFrame(columns=self.dataColumns)
        self.oneMinDf = pd.DataFrame(columns=self.dataColumns)
        self.resampledDict = dict()
        self.resampleFrequency = '5T'
        self.indicators = {'supertrend': {}, 'rsi': {}}
        self.supertrendLength = 7
        self.supertrendMultiplier = 3
        self.rsiPeriod = 14
        self.indicatorValuesToBeAvailable = 45
        self.rsiOversoldLevel = 25
        self.rsiOverboughtLevel = 80
        self.rsiBuyExitLevel = 55
        self.rsiSellExitLevel = 45

        # get historical data
        historicalData = self.getBroker().getHistoricalData(self.underlying, datetime.datetime.now() -
                                                            datetime.timedelta(days=20),
                                                            self.resampleFrequency.replace("T", ""))

        self.indicators['supertrend'][self.underlying] = SuperTrend(
            self.supertrendLength, self.supertrendMultiplier)
        self.indicators['rsi'][self.underlying] = RSI(
            self.rsiPeriod)

        for index, row in historicalData.iterrows():
            self.addIndicators(row['Date/Time'], row['Open'], row['High'],
                               row['Low'], row['Close'], row['Volume'], row['Open Interest'])

        self.resampleBarFeed(
            5 * pyalgotrade.bar.Frequency.MINUTE, self.on5MinBars)

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionBullish = None
        self.positionBearish = None

    def addIndicators(self, dateTime, open, high, low, close, volume, openInterest):
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
            dateTime)
        self.resampledDict[self.underlying]['Open'].append(open)
        self.resampledDict[self.underlying]['High'].append(high)
        self.resampledDict[self.underlying]['Low'].append(low)
        self.resampledDict[self.underlying]['Close'].append(close)
        self.resampledDict[self.underlying]['Volume'].append(volume)
        self.resampledDict[self.underlying]['Open Interest'].append(
            openInterest)

        ohlcv = OHLCV(open, high, low,
                      close, volume, dateTime)

        self.indicators['supertrend'][self.underlying].add(ohlcv)
        self.indicators['rsi'][self.underlying].add(ohlcv.close)

        if ((len(self.indicators['supertrend'][self.underlying]) < self.indicatorValuesToBeAvailable)
                or (len(self.indicators['rsi'][self.underlying]) < self.indicatorValuesToBeAvailable)):
            return

        self.log(f"{dateTime} - LTP <{self.resampledDict[self.underlying]['Close'][-1]}> "
                 f"Supertrend <{self.indicators['supertrend'][self.underlying][-1].trend}> "
                 f"<{self.indicators['supertrend'][self.underlying][-1].value}> RSI "
                 f"<{self.indicators['rsi'][self.underlying][-1]}>", logging.DEBUG)

    def on5MinBars(self, bars: pyalgotrade.bar.Bars):
        bar = bars.getBar(self.underlying)

        if bar is None:
            return

        self.addIndicators(bar.getDateTime(), bar.getOpen(), bar.getHigh(), bar.getLow(),
                           bar.getClose(), bar.getVolume(), bar.getExtraColumns().get("Open Interest", 0))

        if (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            if ((len(self.indicators['supertrend'][self.underlying]) < self.indicatorValuesToBeAvailable)
                    or (len(self.indicators['rsi'][self.underlying]) < self.indicatorValuesToBeAvailable)):
                return
            currentExpiry = utils.getNearestWeeklyExpiryDate(
                bars.getDateTime().date())

            supertrendValue = self.indicators['supertrend'][self.underlying][-1]
            rsiValue = self.indicators['rsi'][self.underlying][-1]
            lastClose = self.resampledDict[self.underlying]['Close'][-1]

            self.log(
                f'{bars.getDateTime()} - {self.underlying} - LTP <{lastClose}> Supertrend '
                f'<{supertrendValue.value}> RSI <{rsiValue}>', logging.DEBUG)

            if (supertrendValue.trend == Trend.UP) and (self.rsiBuyExitLevel < rsiValue < self.rsiOverboughtLevel):
                if self.positionBearish is not None:
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend <{supertrendValue.value}> is UP and '
                        f'RSI <{rsiValue}> is between <{self.rsiBuyExitLevel}> & <{self.rsiOverboughtLevel}>. '
                        f'Exiting last position')
                    self.positionBearish.exitMarket()
                    self.positionBearish = None
                if self.positionBullish is None:
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), 100)
                    symbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'c')
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend <{supertrendValue.value}> is UP and'
                        f' RSI <{rsiValue}> is between <{self.rsiBuyExitLevel}> & <{self.rsiOverboughtLevel}>. '
                        f'Entering {symbol} Long')
                    self.state = State.PLACING_ORDERS
                    self.positionBullish = self.enterLong(
                        symbol, self.quantity)
            elif (supertrendValue.trend == Trend.DOWN) and (self.rsiOversoldLevel < rsiValue < self.rsiSellExitLevel):
                if self.positionBullish is not None:
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend <{supertrendValue.value}> is DOWN and '
                        f'RSI <{rsiValue}> is between <{self.rsiOversoldLevel}> & '
                        f'<{self.rsiSellExitLevel}>. Exiting last position')
                    self.positionBullish.exitMarket()
                    self.positionBullish = None
                if self.positionBearish is None:
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), 100)
                    symbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'p')
                    self.log(
                        f'{bars.getDateTime()} - Supertrend trend <{supertrendValue.value}> is DOWN and RSI '
                        f'<{rsiValue}> is between <{self.rsiOversoldLevel}> & <{self.rsiSellExitLevel}>. '
                        f'Entering {symbol} Long')
                    self.state = State.PLACING_ORDERS
                    self.positionBearish = self.enterLong(
                        symbol, self.quantity)
        elif self.state == State.ENTERED:
            supertrendValue = self.indicators['supertrend'][self.underlying][-1]
            rsiValue = self.indicators['rsi'][self.underlying][-1]
            lastClose = self.resampledDict[self.underlying]['Close'][-1]

            if (self.positionBullish is not None) and ((supertrendValue.trend == Trend.DOWN)
                                                       or (rsiValue < self.rsiBuyExitLevel)):
                self.log(
                    f'{bars.getDateTime()} - Supertrend trend <{supertrendValue.value}> is '
                    f'{supertrendValue.trend} or RSI <{rsiValue}> < {self.rsiBuyExitLevel}. Exiting bullish position!')
                self.state = State.PLACING_ORDERS
                self.positionBullish.exitMarket()
                self.positionBullish = None
            elif (self.positionBearish is not None) and ((supertrendValue.trend == Trend.UP)
                                                         or (rsiValue > self.rsiSellExitLevel)):
                self.log(
                    f'{bars.getDateTime()} - Supertrend trend <{supertrendValue.value}> is '
                    f'{supertrendValue.trend} or RSI <{rsiValue}> > {self.rsiSellExitLevel}. Exiting bearish position!')
                self.state = State.PLACING_ORDERS
                self.positionBearish.exitMarket()
                self.positionBearish = None

    def closeAllPositions(self):
        if self.state == State.EXITED:
            return

        self.state = State.EXITED
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()
        self.positionBearish = self.positionBullish = None

    def onBars(self, bars: pyalgotrade.bar.Bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.INFO)

        self.overallPnL = self.getOverallPnL()

        if bars.getDateTime().time() >= self.marketEndTime:
            # TODO: This is not exiting even at market close time
            if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        # Exit all positions if exit time is met or portfolio SL is hit
        elif bars.getDateTime().time() >= self.exitTime:
            if (self.state != State.EXITED) and (len(self.getActivePositions()) > 0):
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit '
                    f'time <{self.exitTime}. Closing all positions!')
                self.closeAllPositions()
        elif self.overallPnL <= -self.portfolioSL:
            if self.state != State.EXITED:
                self.log(
                    f'Current PnL <{self.overallPnL}> has crossed portfolio SL '
                    f'<{self.portfolioSL}>. Closing all positions!')
                self.closeAllPositions()
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif self.state == State.EXITED:
            pass


if __name__ == "__main__":
    from pyalgomate.cli import CliMain
    CliMain(SuperTrendRSIV1)
