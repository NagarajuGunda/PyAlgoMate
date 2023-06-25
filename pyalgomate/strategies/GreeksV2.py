from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State
from pyalgomate.cli import CliMain
import logging
import sys


class GreeksV2(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, registeredOptionsCount=0, callback=None, resampleFrequency=None, lotSize=None, collectData=None):
        super(GreeksV2, self).__init__(feed, broker,
                                       strategyName=__class__.__name__,
                                       logger=logging.getLogger(
                                           __file__),
                                       callback=callback,
                                       collectData=collectData)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.marketMidTime = datetime.time(hour=12, minute=30)
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 2000
        self.underlying = underlying
        self.registeredOptionsCount = registeredOptionsCount
        self.callback = callback
        self.strikeDifference = 100
        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.openingGreeks = {}
        self.maxCeVega = -sys.maxsize - 1
        self.maxPeVega = -sys.maxsize - 1
        self.positionCall = self.positionPut = None
        self.tradesTaken = 0

    def on1MinBars(self, bars):
        if bars.getDateTime().time() == self.marketMidTime:
            self.openingGreeks = {}
            if (self.state != State.EXITED) and (len(self.openPositions) > 0):
                self.log(
                    f'Current time <{bars.getDateTime().time()}> is equal to mid time <{self.marketMidTime}. Closing all positions!')
                for position in list(self.getActivePositions()):
                    if not position.exitActive():
                        position.exitMarket()
                self.positionCall = self.positionPut = None
                self.state = State.LIVE
                self.tradesTaken = 0

        # Calculate sum of change in greeks
        underlyingLTP = self.getLTP(self.underlying)
        atmStrike = self.getATMStrike(underlyingLTP, 100)

        currentExpiry = utils.getNearestWeeklyExpiryDate(
            bars.getDateTime().date())
        nextWeekExpiry = utils.getNextWeeklyExpiryDate(
            datetime.datetime.now().date())
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(
            datetime.datetime.now().date())

        callOTMStrikesGreeks = self.getOTMStrikeGreeks(
            atmStrike - self.strikeDifference, 'c', currentExpiry, 7)
        putOTMStrikesGreeks = self.getOTMStrikeGreeks(
            atmStrike + self.strikeDifference, 'p', currentExpiry, 7)

        callOTMStrikesGreeks += self.getOTMStrikeGreeks(
            atmStrike - self.strikeDifference, 'c', nextWeekExpiry, 7)
        putOTMStrikesGreeks += self.getOTMStrikeGreeks(
            atmStrike + self.strikeDifference, 'p', nextWeekExpiry, 7)

        if currentExpiry != monthlyExpiry:
            callOTMStrikesGreeks += self.getOTMStrikeGreeks(
                atmStrike - self.strikeDifference, 'c', monthlyExpiry, 7)
            putOTMStrikesGreeks += self.getOTMStrikeGreeks(
                atmStrike + self.strikeDifference, 'p', monthlyExpiry, 7)

        if len(callOTMStrikesGreeks) == 0 or len(putOTMStrikesGreeks) == 0 or len(self.openingGreeks) == 0:
            return super().on1MinBars(bars)

        callSumOfChangeInGreeks = sum(
            [(greek.vega - self.openingGreeks[greek.optionContract.symbol].vega if greek.optionContract.symbol in self.openingGreeks else greek.vega) for greek in callOTMStrikesGreeks])
        putSumOfChangeInGreeks = sum(
            [(greek.vega - self.openingGreeks[greek.optionContract.symbol].vega if greek.optionContract.symbol in self.openingGreeks else greek.vega) for greek in putOTMStrikesGreeks])

        callSumOfVega = sum(
            [greek.vega for greek in callOTMStrikesGreeks])
        putSumOfVega = sum(
            [greek.vega for greek in putOTMStrikesGreeks])

        self.log(
            f'Bar Date/Time <{bars.getDateTime()}> LTP <{underlyingLTP}> Sum (CE <{callSumOfVega}>, PE <{putSumOfVega}>) Change (Ce <{callSumOfChangeInGreeks}>, PE<{putSumOfChangeInGreeks}>)', logging.INFO)

        if callSumOfChangeInGreeks > self.maxCeVega:
            self.maxCeVega = callSumOfChangeInGreeks
        if putSumOfChangeInGreeks > self.maxPeVega:
            self.maxPeVega = putSumOfChangeInGreeks

        if self.callback is not None:
            jsonData = {
                "datetime": bars.getDateTime().strftime('%Y-%m-%d %H:%M:%S'),
                "charts": {
                    "CeVega": callSumOfChangeInGreeks,
                    "PeVega": putSumOfChangeInGreeks,
                    "MaxCEVega": self.maxCeVega,
                    "MaxPEVega": self.maxPeVega
                }
            }

            self.callback(self.strategyName, jsonData)

        if (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            if self.tradesTaken == 0:
                # if sum of change in vega is positive, that means, vega has increased in options which implies buying is happening
                # if sum of change in vega is negative, that means, vega has decayed in options which implies selling is happening
                if callSumOfChangeInGreeks < -10 and putSumOfChangeInGreeks < -10:
                    self.state = State.PLACING_ORDERS
                    if (callSumOfChangeInGreeks / putSumOfChangeInGreeks) > 2:
                        self.positionPut = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'p'), self.quantity)
                    elif (putSumOfChangeInGreeks/callSumOfChangeInGreeks) > 2:
                        self.positionCall = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'c'), self.quantity)
                    self.tradesTaken += 1
                elif callSumOfChangeInGreeks > 10 and putSumOfChangeInGreeks > 10:
                    self.state = State.PLACING_ORDERS
                    if (callSumOfChangeInGreeks / putSumOfChangeInGreeks) > 2:
                        self.positionCall = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'c'), self.quantity)
                    elif (putSumOfChangeInGreeks/callSumOfChangeInGreeks) > 2:
                        self.positionPut = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'p'), self.quantity)
                    self.tradesTaken += 1
                elif callSumOfChangeInGreeks < -10 and putSumOfChangeInGreeks > 0:
                    self.state = State.PLACING_ORDERS
                    self.positionPut = self.enterLong(self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'p'), self.quantity)
                    self.tradesTaken += 1
                elif putSumOfChangeInGreeks < -10 and callSumOfChangeInGreeks > 0:
                    self.state = State.PLACING_ORDERS
                    self.positionCall = self.enterLong(self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'c'), self.quantity)
                    self.tradesTaken += 1

        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return

        return super().on1MinBars(bars)

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        optionData = self.getOptionData(bars)
        # self.log(f'registeredOptionsCount <{self.registeredOptionsCount}> optionData <{len(optionData)}>')

        for instrument, value in optionData.items():
            if instrument not in self.openingGreeks:
                self.openingGreeks[instrument] = value

        if (self.registeredOptionsCount > 0) and (len(optionData) < self.registeredOptionsCount):
            return

        self.overallPnL = self.getOverallPnL()

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.openPositions) + len(self.closedPositions)) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        elif (bars.getDateTime().time() >= self.exitTime):
            if (self.state != State.EXITED) and (len(self.openPositions) > 0):
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time <{self.exitTime}. Closing all positions!')
                for position in list(self.getActivePositions()):
                    if not position.exitActive():
                        position.exitMarket()
                self.positionCall = self.positionPut = None
                self.state = State.EXITED


if __name__ == "__main__":
    CliMain(GreeksV2)
