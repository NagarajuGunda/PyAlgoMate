from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.core import State
from pyalgomate.cli import CliMain
import logging


class GreeksV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, registeredOptionsCount=0,
                 callback=None, lotSize=None, collectData=None, telegramBot=None):
        super(GreeksV1, self).__init__(feed, broker,
                                       strategyName=strategyName if strategyName else __class__.__name__,
                                       logger=logging.getLogger(
                                           __file__),
                                       callback=callback,
                                       collectData=collectData,
                                       telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 2000
        self.underlying = underlying
        self.registeredOptionsCount = registeredOptionsCount
        self.callback = callback
        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.openingGreeks = {}
        self.positionCall = self.positionPut = None

    def __str__(self):
        return 'GreeksV1'

    def on1MinBars(self, bars):
        # Calculate sum of change in greeks
        underlyingLTP = self.getLTP(self.underlying)
        atmStrike = self.getATMStrike(underlyingLTP, 100)

        currentExpiry = utils.getNearestWeeklyExpiryDate(
            bars.getDateTime().date())

        callOTMStrikesGreeks = self.getOTMStrikeGreeks(
            atmStrike, 'c', currentExpiry)
        putOTMStrikesGreeks = self.getOTMStrikeGreeks(
            atmStrike, 'p', currentExpiry)

        if len(callOTMStrikesGreeks) == 0 or len(putOTMStrikesGreeks) == 0 or len(self.openingGreeks) == 0:
            return super().on1MinBars(bars)

        callSumOfChangeInGreeks = sum(
            [(greek.vega - self.openingGreeks[
                greek.optionContract.symbol].vega if greek.optionContract.symbol in self.openingGreeks else greek.vega)
             for greek in callOTMStrikesGreeks])
        putSumOfChangeInGreeks = sum(
            [(greek.vega - self.openingGreeks[
                greek.optionContract.symbol].vega if greek.optionContract.symbol in self.openingGreeks else greek.vega)
             for greek in putOTMStrikesGreeks])

        self.log(
            f'Sum of change in vega - Calls <{callSumOfChangeInGreeks}>, Puts <{putSumOfChangeInGreeks}>', logging.INFO)

        if self.callback is not None:
            jsonData = {
                "datetime": bars.getDateTime().strftime('%Y-%m-%d %H:%M:%S'),
                "charts": {
                    "CeVega": callSumOfChangeInGreeks,
                    "PeVega": putSumOfChangeInGreeks
                }
            }

            self.callback(self.strategyName, jsonData)

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        elif (bars.getDateTime().time() >= self.exitTime):
            if (self.state != State.EXITED) and (len(self.getActivePositions()) > 0):
                self.log(
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time '
                    f'<{self.exitTime}. Closing all positions!')
                for position in list(self.getActivePositions()):
                    if not position.exitActive():
                        position.exitMarket()
                self.positionCall = self.positionPut = None
                self.state = State.EXITED
        if (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            if self.positionCall is None and self.positionPut is None:
                # if sum of change in vega is positive, that means, vega has increased in options which implies
                # buying is happening if sum of change in vega is negative, that means, vega has decayed in options
                # which implies selling is happening
                if callSumOfChangeInGreeks < -10 and putSumOfChangeInGreeks > 0:
                    self.state = State.PLACING_ORDERS
                    self.positionPut = self.enterLong(self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'p'), self.quantity)
                elif putSumOfChangeInGreeks < -10 and callSumOfChangeInGreeks > 0:
                    self.state = State.PLACING_ORDERS
                    self.positionCall = self.enterLong(self.getOptionSymbol(
                        self.underlying, currentExpiry, atmStrike, 'c'), self.quantity)
                elif callSumOfChangeInGreeks < -10 and putSumOfChangeInGreeks < -10:
                    self.state = State.PLACING_ORDERS
                    if (callSumOfChangeInGreeks / putSumOfChangeInGreeks) > 2:
                        self.positionPut = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'p'), self.quantity)
                    elif (putSumOfChangeInGreeks / callSumOfChangeInGreeks) > 2:
                        self.positionCall = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'c'), self.quantity)
                elif callSumOfChangeInGreeks > 10 and putSumOfChangeInGreeks > 10:
                    self.state = State.PLACING_ORDERS
                    if (callSumOfChangeInGreeks / putSumOfChangeInGreeks) > 2:
                        self.positionCall = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'c'), self.quantity)
                    elif (putSumOfChangeInGreeks / callSumOfChangeInGreeks) > 2:
                        self.positionPut = self.enterLong(self.getOptionSymbol(
                            self.underlying, currentExpiry, atmStrike, 'p'), self.quantity)
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
        if (self.registeredOptionsCount > 0) and (len(optionData) < self.registeredOptionsCount):
            return

        for instrument, value in optionData.items():
            if instrument not in self.openingGreeks:
                self.openingGreeks[instrument] = value

        self.overallPnL = self.getOverallPnL()


if __name__ == "__main__":
    CliMain(GreeksV1)
