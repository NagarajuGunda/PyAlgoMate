import logging
import datetime
from talipp.indicators import SuperTrend
from talipp.indicators.SuperTrend import Trend
from talipp.ohlcv import OHLCVFactory, OHLCV
import pandas as pd

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State, Expiry

logger = logging.getLogger(__file__)


class SuperTrendV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, registeredOptionsCount=None, callback=None, resampleFrequency=None, lotSize=None, collectData=None):
        super(SuperTrendV1, self).__init__(feed, broker,
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

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionBullish = None
        self.positionBearish = None

    def setUnderlying(self, underlying):
        self.underlying = underlying

    def on1MinBars(self, bars):
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

        super().on1MinBars(bars)

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
