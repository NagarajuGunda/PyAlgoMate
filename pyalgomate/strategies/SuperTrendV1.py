import logging
import datetime
import pandas as pd
import pandas_ta as ta
import calendar

import pyalgomate.utils as utils
from pyalgotrade import strategy
from pyalgotrade import bar
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
        self.resampleFrequency = '1T'
        self.supertrend = dict()
        self.supertrendLength = 7
        self.supertrendMultiplier = 3

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
        newRows = []
        for ticker, bar in bars.items():
            newRow = {
                "Ticker": ticker,
                "Date/Time": bar.getDateTime(),
                "Open": bar.getOpen(),
                "High": bar.getHigh(),
                "Low": bar.getLow(),
                "Close": bar.getClose(),
                "Volume": bar.getVolume(),
                "Open Interest": bar.getExtraColumns().get("Open Interest", 0)
            }

            newRows.append(newRow)

        self.oneMinDf = pd.concat([self.oneMinDf, pd.DataFrame(
            newRows, columns=self.dataColumns)], ignore_index=True)

        underlyingDf = self.oneMinDf[self.oneMinDf['Ticker']
                                     == self.underlying]
        supertrend = ta.supertrend(underlyingDf['High'], underlyingDf['Low'], underlyingDf['Close'],
                                   length=self.supertrendLength, multiplier=self.supertrendMultiplier)
        if supertrend is not None:
            self.supertrend[ticker] = supertrend.iloc[:, 0].values
            self.log(
                f'{bars.getDateTime()} - {ticker} - LTP <{underlyingDf.Close.values[-1]}> Supertrend <{self.supertrend[ticker][-1]}>', logging.DEBUG)

            # Green
            if underlyingDf.Close.values[-1] > self.supertrend[ticker][-1]:
                if self.positionBearish is not None:
                    self.log(
                        f'{bars.getDateTime()} - Close greater than supertrend and so supertrend is green. Exiting last position')
                    self.positionBearish.exitMarket()
                    self.positionBearish = None
                if self.positionBullish is None:
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), 100)
                    peSymbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'p')
                    self.log(
                        f'{bars.getDateTime()} - Close greater than supertrend and so supertrend is green. Entering PE {peSymbol} short')
                    self.positionBullish = self.enterShort(
                        peSymbol, self.quantity)
            elif underlyingDf.Close.values[-1] < self.supertrend[ticker][-1]:
                if self.positionBullish is not None:
                    self.log(
                        f'{bars.getDateTime()} - Close lesser than supertrend and so supertrend is red. Exiting last position')
                    self.positionBullish.exitMarket()
                    self.positionBullish = None
                if self.positionBearish is None:
                    atmStrike = self.getATMStrike(
                        self.getLTP(self.underlying), 100)
                    ceSymbol = self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'c')
                    self.log(
                        f'{bars.getDateTime()} - Close lesser than supertrend and so supertrend is red. Entering CE {ceSymbol} short')
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
