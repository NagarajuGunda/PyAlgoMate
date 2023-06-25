from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
import pyalgomate.utils as utils
import datetime
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State
from pyalgomate.cli import CliMain
import logging
import pyalgotrade.bar

'''
Buy ATM straddle in current expiry
Sell OTM strangle in monthly expiry
'''


class SpreadsV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, registeredOptionsCount=0, callback=None, resampleFrequency=None, lotSize=None, collectData=None):
        super(SpreadsV1, self).__init__(feed, broker,
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

        self.portfolioProfit = 2000
        self.portfolioSL = 1000
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
        currentMonthExpiry = utils.getNearestMonthlyExpiryDate(
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
            otmCEMonthly = self.getOptionSymbol(
                self.underlying, currentMonthExpiry, atmStrike + (4 * self.strikeDifference), 'c')
            otmPEMonthly = self.getOptionSymbol(
                self.underlying, currentMonthExpiry, atmStrike - (4 * self.strikeDifference), 'p')

            if self.getLTP(atmCEWeekly) is None or self.getLTP(atmPEWeekly) is None or self.getLTP(otmCEMonthly) is None or self.getLTP(otmPEMonthly) is None:
                return

            self.state = State.PLACING_ORDERS
            self.positions.append(self.enterLong(otmCEMonthly, self.quantity))
            self.positions.append(self.enterLong(otmPEMonthly, self.quantity))
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
    CliMain(SpreadsV1)
