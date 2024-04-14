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
import log_setup  # noqa

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State


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

    def getCombinedPremiumBar(self, dateTime, strike, name):
        currentExpiry = utils.getNearestWeeklyExpiryDate(
            dateTime.date())

        ceSymbol = self.__strategy.getOptionSymbol(
            self.__strategy.getUnderlying(), currentExpiry, strike, 'c')
        peSymbol = self.__strategy.getOptionSymbol(
            self.__strategy.getUnderlying(), currentExpiry, strike, 'p')

        ceBar = self.__strategy.getFeed().getLastBar(ceSymbol)
        peBar = self.__strategy.getFeed().getLastBar(peSymbol)

        if ceBar is None or peBar is None:
            return None

        dayMonthYear = f"{currentExpiry.day:02d}" + \
                       calendar.month_abbr[currentExpiry.month].upper(
                       ) + str(currentExpiry.year % 100)

        ticker = f'{self.__strategy.getUnderlying()}{dayMonthYear}{name}'

        return bar.BasicBar(dateTime,
                            ceBar.getOpen() + peBar.getOpen(),
                            ceBar.getHigh() + peBar.getHigh(),
                            ceBar.getLow() + peBar.getLow(),
                            ceBar.getClose() + peBar.getClose(),
                            ceBar.getVolume() + peBar.getVolume(),
                            None,
                            bar.Frequency.TRADE,
                            {
                                "Instrument": ticker,
                                "Open Interest": 0,
                                "Date/Time": None
                            })

    def getATMStraddleBar(self, dateTime):
        atmStrike = self.__strategy.getATMStrike(
            self.__strategy.getLTP(self.__strategy.getUnderlying()), 100)
        return self.getCombinedPremiumBar(dateTime, atmStrike, 'ROLLINGATMSTRADDLE')

    def newBars(self, dateTime, bars):
        underlyingBar = bars.getBar(self.__strategy.getUnderlying())

        if underlyingBar is None:
            return

        barDict = {
            self.__strategy.getUnderlying(): underlyingBar}

        combinedBar = self.getATMStraddleBar(dateTime)

        if combinedBar is not None:
            barDict[combinedBar.getExtraColumns()['Instrument']] = combinedBar

        if self.__strategy.atmStrike is not None:
            combinedATMBar = self.getCombinedPremiumBar(
                dateTime, self.__strategy.atmStrike, f'ATM{self.__strategy.atmStrike}COMBINED')

            if combinedATMBar is not None:
                barDict[combinedATMBar.getExtraColumns()['Instrument']
                ] = combinedATMBar

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
    def __init__(self, feed, broker, underlying,
                 strategyName=None,
                 callback=None,
                 collectData=None,
                 telegramBot=None,
                 telegramChannelId=None,
                 telegramMessageThreadId=None):
        super(ATMStraddleV1, self).__init__(feed, broker,
                                            strategyName=strategyName if strategyName else __class__.__name__,
                                            logger=logging.getLogger(
                                                __file__),
                                            callback=callback,
                                            collectData=collectData,
                                            telegramBot=telegramBot,
                                            telegramChannelId=telegramChannelId,
                                            telegramMessageThreadId=telegramMessageThreadId)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.lots = 1
        self.portfolioSL = 2000
        self.underlying = underlying
        underlyingDetails = self.getBroker().getUnderlyingDetails(self.underlying)
        self.underlyingIndex = underlyingDetails['index']
        self.strikeDifference = underlyingDetails['strikeDifference']
        self.lotSize = underlyingDetails['lotSize']
        self.quantity = self.lotSize * self.lots
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

        # get historical data
        historicalData = self.getBroker().getHistoricalData(self.underlying, datetime.datetime.now() -
                                                            datetime.timedelta(days=20),
                                                            self.resampleFrequency.replace("T", ""))

        for index, row in historicalData.iterrows():
            self.addSuperTrend(row['Date/Time'], row['Open'], row['High'],
                               row['Low'], row['Close'], row['Volume'], row['Open Interest'])

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionBullish = None
        self.positionBearish = None
        self.atmStrike = None

    def addSuperTrend(self, dateTime, open, high, low, close, volume, openInterest):
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

        if self.underlying not in self.supertrend:
            self.supertrend[self.underlying] = SuperTrend(
                self.supertrendLength, self.supertrendMultiplier)

        ohlcv = OHLCV(open, high, low,
                      close, volume, dateTime)

        self.supertrend[self.underlying].add(ohlcv)

    def getUnderlying(self):
        return self.underlying

    def onResampledBars(self, bars):
        bar = bars.getBar(self.underlying)

        if bar is None:
            return

        self.addSuperTrend(bar.getDateTime(), bar.getOpen(), bar.getHigh(), bar.getLow(),
                           bar.getClose(), bar.getVolume(), bar.getExtraColumns().get("Open Interest", 0))

        super().on1MinBars(bars)

    def closeAllPositions(self):
        if self.state == State.EXITED:
            return

        self.state = State.EXITED
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()
        self.positionBearish = self.positionBullish = None

    def onBars(self, bars):
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
                    f'Current PnL <{self.overallPnL}> has crossed potfolio SL <{self.portfolioSL}>.'
                    f' Closing all positions!')
                self.closeAllPositions()
        elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            if self.atmStrike is None:
                self.atmStrike = self.getATMStrike(
                    self.getLTP(self.getUnderlying()), 100)

            if self.underlying in self.supertrend and len(
                    self.supertrend[self.underlying]) > self.indicatorValuesToBeAvailable:
                currentExpiry = utils.getNearestWeeklyExpiryDate(
                    bars.getDateTime().date(), self.underlyingIndex)
                supertrendValue = self.supertrend[self.underlying][-1]
                lastClose = self.resampledDict[self.underlying]['Close'][-1]
                self.log(
                    f'{bars.getDateTime()} - {self.underlying} - LTP <{lastClose}> Supertrend <{supertrendValue.value}>',
                    logging.DEBUG)

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
    from pyalgomate.cli import CliMain

    CliMain(ATMStraddleV1)
