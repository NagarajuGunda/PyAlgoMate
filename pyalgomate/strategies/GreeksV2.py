from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.core import State
from pyalgomate.cli import CliMain
import logging
import sys

class AdvancedGreeksStrategy(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, registeredOptionsCount=0,
                 callback=None, lotSize=None, collectData=None, telegramBot=None):
        super().__init__(feed, broker, 
                         strategyName=strategyName if strategyName else self.__class__.__name__,
                         logger=logging.getLogger(__file__), 
                         callback=callback, 
                         collectData=collectData, 
                         telegramBot=telegramBot)
        
        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.marketMidTime = datetime.time(hour=12, minute=30)
        self.lotSize = lotSize or 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 2000
        self.underlying = underlying
        self.registeredOptionsCount = registeredOptionsCount
        self.callback = callback
        self.strikeDifference = 100
        self.reset()

    def reset(self):
        super().reset()
        self.openingGreeks = {}
        self.maxCeVega = -sys.maxsize
        self.maxPeVega = -sys.maxsize
        self.positionCall = None
        self.positionPut = None
        self.tradesTaken = 0
        self.overallPnL = 0

    def calculateSumOfChangeInGreeks(self, greeksList, openingGreeks):
        return sum((greek.vega - openingGreeks[greek.optionContract.symbol].vega
                    if greek.optionContract.symbol in openingGreeks else greek.vega) for greek in greeksList)

    def calculateSumOfVega(self, greeksList):
        return sum(greek.vega for greek in greeksList)

    def on1MinBars(self, bars):
        currentTime = bars.getDateTime().time()
        if currentTime == self.marketMidTime:
            self.closeAllPositions()
            self.resetMidDayState()

        underlyingLTP = self.getLTP(self.underlying)
        atmStrike = self.getATMStrike(underlyingLTP, self.strikeDifference)
        expiryDates = self.getRelevantExpiryDates(bars)

        callGreeks, putGreeks = self.getGreeksForStrikes(atmStrike, expiryDates)
        
        if not callGreeks or not putGreeks or not self.openingGreeks:
            return super().on1MinBars(bars)

        callChangeInGreeks = self.calculateSumOfChangeInGreeks(callGreeks, self.openingGreeks)
        putChangeInGreeks = self.calculateSumOfChangeInGreeks(putGreeks, self.openingGreeks)

        callSumOfVega = self.calculateSumOfVega(callGreeks)
        putSumOfVega = self.calculateSumOfVega(putGreeks)

        self.logGreeksData(bars, underlyingLTP, callSumOfVega, putSumOfVega, callChangeInGreeks, putChangeInGreeks)

        self.updateMaxVega(callChangeInGreeks, putChangeInGreeks)
        self.handleCallback(bars, callChangeInGreeks, putChangeInGreeks)

        if self.state == State.LIVE and self.entryTime <= currentTime < self.exitTime:
            self.evaluateEntryConditions(callChangeInGreeks, putChangeInGreeks, atmStrike, expiryDates['current'])

        elif self.state == State.PLACING_ORDERS:
            self.evaluatePlacingOrdersState()

        return super().on1MinBars(bars)

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)
        optionData = self.getOptionData(bars)
        self.initializeOpeningGreeks(optionData)

        if self.registeredOptionsCount and len(optionData) < self.registeredOptionsCount:
            return

        self.overallPnL = self.getOverallPnL()
        self.handleMarketEnd(bars)

        if bars.getDateTime().time() >= self.exitTime:
            self.closePositionsOnExitTime(bars)

    def closeAllPositions(self):
        if self.state != State.EXITED and self.getActivePositions():
            for position in self.getActivePositions():
                if not position.exitActive():
                    position.exitMarket()
            self.positionCall = None
            self.positionPut = None
            self.state = State.EXITED

    def resetMidDayState(self):
        self.openingGreeks = {}
        self.tradesTaken = 0
        self.state = State.LIVE

    def getRelevantExpiryDates(self, bars):
        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date())
        nextWeekExpiry = utils.getNextWeeklyExpiryDate(datetime.datetime.now().date())
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(datetime.datetime.now().date())
        return {'current': currentExpiry, 'next': nextWeekExpiry, 'monthly': monthlyExpiry}

    def getGreeksForStrikes(self, atmStrike, expiryDates):
        callGreeks = self.getOTMStrikeGreeks(atmStrike - self.strikeDifference, 'c', expiryDates['current'], 7)
        putGreeks = self.getOTMStrikeGreeks(atmStrike + self.strikeDifference, 'p', expiryDates['current'], 7)

        callGreeks += self.getOTMStrikeGreeks(atmStrike - self.strikeDifference, 'c', expiryDates['next'], 7)
        putGreeks += self.getOTMStrikeGreeks(atmStrike + self.strikeDifference, 'p', expiryDates['next'], 7)

        if expiryDates['current'] != expiryDates['monthly']:
            callGreeks += self.getOTMStrikeGreeks(atmStrike - self.strikeDifference, 'c', expiryDates['monthly'], 7)
            putGreeks += self.getOTMStrikeGreeks(atmStrike + self.strikeDifference, 'p', expiryDates['monthly'], 7)

        return callGreeks, putGreeks

    def logGreeksData(self, bars, underlyingLTP, callSumOfVega, putSumOfVega, callChangeInGreeks, putChangeInGreeks):
        self.log(
            f'Bar Date/Time <{bars.getDateTime()}> LTP <{underlyingLTP}> Sum (CE <{callSumOfVega}>, '
            f'PE <{putSumOfVega}>) Change (CE <{callChangeInGreeks}>, PE <{putChangeInGreeks}>)', logging.INFO)

    def updateMaxVega(self, callChangeInGreeks, putChangeInGreeks):
        self.maxCeVega = max(self.maxCeVega, callChangeInGreeks)
        self.maxPeVega = max(self.maxPeVega, putChangeInGreeks)

    def handleCallback(self, bars, callChangeInGreeks, putChangeInGreeks):
        if self.callback:
            jsonData = {
                "datetime": bars.getDateTime().strftime('%Y-%m-%d %H:%M:%S'),
                "charts": {
                    "CeVega": callChangeInGreeks,
                    "PeVega": putChangeInGreeks,
                    "MaxCEVega": self.maxCeVega,
                    "MaxPEVega": self.maxPeVega
                }
            }
            self.callback(self.strategyName, jsonData)
            
# if sum of change in vega is positive, that means, vega has increased in options which implies
# buying is happening if sum of change in vega is negative, that means, vega has decayed in options
# which implies selling is happening

    def evaluateEntryConditions(self, callChangeInGreeks, putChangeInGreeks, atmStrike, currentExpiry):
        if self.tradesTaken == 0:
            if callChangeInGreeks < -10 and putChangeInGreeks < -10:
                self.placeOrder(callChangeInGreeks, putChangeInGreeks, atmStrike, currentExpiry, 'p', 'c')
            elif callChangeInGreeks > 10 and putChangeInGreeks > 10:
                self.placeOrder(callChangeInGreeks, putChangeInGreeks, atmStrike, currentExpiry, 'c', 'p')
            elif callChangeInGreeks < -10 < putChangeInGreeks:
                self.placeSingleOrder(atmStrike, currentExpiry, 'p')
            elif putChangeInGreeks < -10 < callChangeInGreeks:
                self.placeSingleOrder(atmStrike, currentExpiry, 'c')

    def placeOrder(self, callChangeInGreeks, putChangeInGreeks, atmStrike, currentExpiry, firstSymbol, secondSymbol):
        self.state = State.PLACING_ORDERS
        if (callChangeInGreeks / putChangeInGreeks) > 2:
            self.positionPut = self.enterLong(self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, firstSymbol), self.quantity)
        elif (putChangeInGreeks / callChangeInGreeks) > 2:
            self.positionCall = self.enterLong(self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, secondSymbol), self.quantity)
        self.tradesTaken += 1

    def placeSingleOrder(self, atmStrike, currentExpiry, symbol):
        self.state = State.PLACING_ORDERS
        if symbol == 'p':
            self.positionPut = self.enterLong(self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, symbol), self.quantity)
        else:
            self.positionCall = self.enterLong(self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, symbol), self.quantity)
        self.tradesTaken += 1

    def evaluatePlacingOrdersState(self):
        if not self.getActivePositions():
            self.state = State.LIVE
        elif self.isPendingOrdersCompleted():
            self.state = State.ENTERED

    def initializeOpeningGreeks(self, optionData):
        for instrument, value in optionData.items():
            if instrument not in self.openingGreeks:
                self.openingGreeks[instrument] = value

    def handleMarketEnd(self, bars):
        if bars.getDateTime().time() >= self.marketEndTime:
            if self.getActivePositions() or self.getClosedPositions():
                self.log(f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.reset()

    def closePositionsOnExitTime(self, bars):
        self.closeAllPositions()

if __name__ == "__main__":
    CliMain(AdvancedGreeksStrategy)
