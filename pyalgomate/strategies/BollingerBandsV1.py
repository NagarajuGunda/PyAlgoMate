import logging
import datetime
from talipp.indicators import BB
from talipp.indicators.SuperTrend import Trend
from talipp.ohlcv import OHLCV
import pandas as pd

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State
from pyalgomate.cli import CliMain
import pyalgotrade.bar

class BollingerBandsV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, callback=None, lotSize=None, collectData=None, telegramBot=None):
        try:
            super().__init__(feed, broker, strategyName=strategyName or __class__.__name__,
                             logger=logging.getLogger(__file__), callback=callback, 
                             collectData=collectData, telegramBot=telegramBot)

            self.entryTime = datetime.time(hour=9, minute=20)
            self.exitTime = datetime.time(hour=15, minute=15)
            self.lotSize = lotSize or 25
            self.lots = 1
            self.quantity = self.lotSize * self.lots
            self.underlying = underlying
            self.strikeDifference = 100

            self._reset()

            self.dataColumns = ["Ticker", "Date/Time", "Open", "High", "Low", "Close", "Volume", "Open Interest"]
            self.tickDf = pd.DataFrame(columns=self.dataColumns)
            self.resampledDf = pd.DataFrame(columns=self.dataColumns)
            self.resampleFrequency = 5
            self.resampledDict = {}
            self.bollingerBands = {}
            self.bollingerBandPeriod = 20
            self.bollingerBandStdDevMultiplier = 2

            self.resampleBarFeed(self.resampleFrequency * pyalgotrade.bar.Frequency.MINUTE, self.onResampledBars)

            historicalData = self.getBroker().getHistoricalData(self.underlying, datetime.datetime.now() - datetime.timedelta(days=20), str(self.resampleFrequency).replace("T", ""))
            for index, row in historicalData.iterrows():
                self._addBollingerBands(row['Date/Time'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], row['Open Interest'])

        except Exception as e:
            self.log(f"Error initializing strategy: {e}", logging.ERROR)

    def _reset(self):
        super().reset()
        self.positionBullish = []
        self.positionBearish = []

    def _addBollingerBands(self, dateTime, open, high, low, close, volume, openInterest):
        try:
            if self.underlying not in self.resampledDict:
                self.resampledDict[self.underlying] = {'Date/Time': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': [], 'Open Interest': []}
            self.resampledDict[self.underlying]['Date/Time'].append(dateTime)
            self.resampledDict[self.underlying]['Open'].append(open)
            self.resampledDict[self.underlying]['High'].append(high)
            self.resampledDict[self.underlying]['Low'].append(low)
            self.resampledDict[self.underlying]['Close'].append(close)
            self.resampledDict[self.underlying]['Volume'].append(volume)
            self.resampledDict[self.underlying]['Open Interest'].append(openInterest)

            if self.underlying not in self.bollingerBands:
                self.bollingerBands[self.underlying] = BB(self.bollingerBandPeriod, self.bollingerBandStdDevMultiplier)
            self.bollingerBands[self.underlying].add(close)

        except Exception as e:
            self.log(f"Error adding Bollinger Bands data: {e}", logging.ERROR)

    def onResampledBars(self, bars):
        try:
            bar = bars.getBar(self.underlying)
            if bar:
                self._addBollingerBands(bar.getDateTime(), bar.getOpen(), bar.getHigh(), bar.getLow(), bar.getClose(), bar.getVolume(), bar.getExtraColumns().get("Open Interest", 0))
        except Exception as e:
            self.log(f"Error processing resampled bars: {e}", logging.ERROR)

    def closeAllPositions(self):
        try:
            for position in list(self.getActivePositions()):
                if not position.exitActive():
                    position.exitMarket()
            self.positionBearish = self.positionBullish = []
        except Exception as e:
            self.log(f"Error closing positions: {e}", logging.ERROR)

    def onBars(self, bars):
        try:
            self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)
            optionData = self.getOptionData(bars)
            self.overallPnL = self.getOverallPnL()
            bar = bars.getBar(self.underlying)

            if not bar:
                return

            if bars.getDateTime().time() >= self.marketEndTime:
                if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                    self.log(f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
                if self.state != State.LIVE:
                    self._reset()
            elif bars.getDateTime().time() >= self.exitTime:
                if self.state != State.EXITED:
                    self.log(f"Current time {bars.getDateTime().time()} is >= Exit time {self.exitTime}. Closing all positions!")
                    self.closeAllPositions()
                    self.state = State.EXITED
            elif self.state == State.PLACING_ORDERS:
                if len(list(self.getActivePositions())) == 0:
                    self.state = State.LIVE
                    return
                if self.isPendingOrdersCompleted():
                    self.state = State.ENTERED
                    return
            elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
                if len(self.bollingerBands[self.underlying]) < 2:
                    return

                currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date())
                if self.bollingerBands[self.underlying][-2] is None:
                    return

                if (self.resampledDict[self.underlying]['Close'][-2] <= self.bollingerBands[self.underlying][-2].ub) and \
                   (self.resampledDict[self.underlying]['Close'][-1] > self.bollingerBands[self.underlying][-1].ub):
                    if bar.getHigh() > self.resampledDict[self.underlying]['High'][-1]:
                        atmStrike = self.getATMStrike(self.getLTP(self.underlying), self.strikeDifference)
                        atmSymbol = self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, 'c')
                        atmLTP = self.getLTP(atmSymbol)

                        if atmLTP is None:
                            self.log(f'{atmSymbol} LTP is not found', logging.WARN)
                            return

                        otmGreeks = self.getNearestPremiumOption('c', atmLTP / 2, currentExpiry)

                        if otmGreeks is None:
                            self.log(f'Nearest <{atmLTP / 2}> premium CE option is not found', logging.WARN)
                            return

                        self.log(f'Found a bullish setup. Initiating a Ratio Backspread')
                        self.state = State.PLACING_ORDERS
                        self.positionBearish.append(self.enterLong(otmGreeks.optionContract.symbol, 2 * self.quantity))
                        self.positionBearish.append(self.enterShort(atmSymbol, self.quantity))
                elif (self.resampledDict[self.underlying]['Close'][-2] >= self.bollingerBands[self.underlying][-2].lb) and \
                     (self.resampledDict[self.underlying]['Close'][-1] < self.bollingerBands[self.underlying][-1].lb):
                    if bar.getLow() < self.resampledDict[self.underlying]['Low'][-1]:
                        atmStrike = self.getATMStrike(self.getLTP(self.underlying), self.strikeDifference)
                        atmSymbol = self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, 'p')
                        atmLTP = self.getLTP(atmSymbol)

                        if atmLTP is None:
                            self.log(f'{atmSymbol} LTP is not found', logging.WARN)
                            return

                        otmGreeks = self.getNearestPremiumOption('p', atmLTP / 2, currentExpiry)

                        if otmGreeks is None:
                            self.log(f'Nearest <{atmLTP / 2}> premium PE option is not found', logging.WARN)
                            return

                        self.log(f'Found a bearish setup. Initiating a Ratio Backspread')
                        self.state = State.PLACING_ORDERS
                        self.positionBearish.append(self.enterLong(otmGreeks.optionContract.symbol, 2 * self.quantity))
                        self.positionBearish.append(self.enterShort(atmSymbol, self.quantity))
            elif self.state == State.ENTERED:
                if len(self.positionBullish) > 0:
                    if self.resampledDict[self.underlying]['Close'][-1] < self.bollingerBands[self.underlying][-1].ub:
                        if bar.getClose() < self.resampledDict[self.underlying]['Low'][-1]:
                            self.log(f'Previous {self.resampleFrequency} Minute candle has closed below Upper BB and price has broken that candle Low. Exiting all positions')
                            self.state = State.PLACING_ORDERS
                            self.closeAllPositions()
                elif len(self.positionBearish) > 0:
                    if self.resampledDict[self.underlying]['Close'][-1] > self.bollingerBands[self.underlying][-1].lb:
                        if bar.getClose() > self.resampledDict[self.underlying]['High'][-1]:
                            self.log(f'Previous {self.resampleFrequency} Minute candle has closed above Lower BB and price has broken that candle High. Exiting all positions')
                            self.state = State.PLACING_ORDERS
                            self.closeAllPositions()
                else:
                    self.state = State.LIVE
        except Exception as e:
            self.log(f"Error in onBars: {e}", logging.ERROR)

if __name__ == "__main__":
    try:
        CliMain(BollingerBandsV1)
    except Exception as e:
        logging.error(f"Error running CLI main: {e}")
