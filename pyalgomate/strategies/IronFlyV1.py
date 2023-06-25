from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State
from pyalgomate.cli import CliMain
import logging
import pyalgotrade.bar

'''
Deploy IronFly at entry time and exit at exit time
'''


class IronFlyV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, registeredOptionsCount=0, callback=None, resampleFrequency=None, lotSize=None, collectData=None):
        super(IronFlyV1, self).__init__(feed, broker,
                                        strategyName=__class__.__name__,
                                        logger=logging.getLogger(
                                            __file__),
                                        callback=callback,
                                        collectData=collectData)

        self.entryTime = datetime.time(hour=9, minute=30)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.underlying = underlying
        self.strikeDifference = 100
        self.registeredOptionsCount = registeredOptionsCount
        self.callback = callback

        self.portfolioProfit = 500
        self.portfolioSL = 500
        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positions = []

    def closeAllPositions(self):
        self.state = State.PLACING_ORDERS
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()
        self.state = State.EXITED

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        self.overallPnL = self.getOverallPnL()

        currentWeekExpiry = utils.getNearestWeeklyExpiryDate(
            bars.getDateTime().date())

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
                self.closeAllPositions()
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            ltp = self.getLTP(self.underlying)

            if ltp is None:
                return

            atmStrike = self.getATMStrike(ltp, self.strikeDifference)
            atmCEWeekly = self.getOptionSymbol(
                self.underlying, currentWeekExpiry, atmStrike, 'c')
            atmPEWeekly = self.getOptionSymbol(
                self.underlying, currentWeekExpiry, atmStrike, 'p')

            atmCELTP = self.getLTP(atmCEWeekly)
            atmPELTP = self.getLTP(atmPEWeekly)
            if atmCELTP is None or atmPELTP is None:
                return

            otmCEWeekly = self.getOptionSymbol(
                self.underlying, currentWeekExpiry, self.getATMStrike(atmStrike + atmCELTP + atmPELTP, self.strikeDifference), 'c')
            otmPEWeekly = self.getOptionSymbol(
                self.underlying, currentWeekExpiry, self.getATMStrike(atmStrike - atmCELTP - atmPELTP, self.strikeDifference), 'p')

            if self.getLTP(otmCEWeekly) is None or self.getLTP(otmPEWeekly) is None:
                return

            self.state = State.PLACING_ORDERS
            self.positions.append(self.enterLong(otmCEWeekly, self.quantity))
            self.positions.append(self.enterLong(otmPEWeekly, self.quantity))
            self.positions.append(self.enterShort(atmCEWeekly, self.quantity))
            self.positions.append(self.enterShort(atmPEWeekly, self.quantity))
        elif self.state == State.ENTERED:
            if self.overallPnL >= self.portfolioProfit:
                self.log(
                    f'Profit <{self.overallPnL}> has reached portfolio take profit <{self.portfolioProfit}>. Exiting all positions')
                self.closeAllPositions()
            elif self.overallPnL <= -self.portfolioSL:
                self.log(
                    f'Loss <{self.overallPnL}> has reached portfolio stop loss <{self.portfolioSL}>. Exiting all positions')
                self.closeAllPositions()


if __name__ == "__main__":
    CliMain(IronFlyV1)
