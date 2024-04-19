import logging
import datetime
from pyalgotrade.strategy import position

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State
from pyalgomate.cli import CliMain

logger = logging.getLogger(__file__)

'''
Enter Straddle around 9:30 
Put 30% SL on both legs
Re execute another straddle at CMP when one side SL is hit and put 30% SL for it also
If one more SL is hit, then put SL of 30% for the left over Strike prices at their CMP. (Example : if the premiums are 120 & 125 at the time when 2nd SL is hit. Put a 30% SL on those premiums.) 
EXIT when 3rd SL is hit or exit time
'''


class StraddleIntradayV2(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying, strategyName=None, callback=None,
                 lotSize=None, collectData=None, telegramBot=None):
        super(StraddleIntradayV2, self).__init__(feed, broker,
                                                 strategyName=strategyName if strategyName else __class__.__name__,
                                                 logger=logging.getLogger(
                                                     __file__),
                                                 callback=callback,
                                                 collectData=collectData,
                                                 telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=9, minute=30)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.marketEndTime = datetime.time(hour=15, minute=30)
        self.underlying = underlying
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.slPercentage = 30

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.initialMaxSLCount = self.maxSLCount = 3
        self.positions = []

    def onEnterOk(self, position: position):
        entryPrice = position.getEntryOrder().getExecutionInfo().getPrice()
        self.positions.append({'instrument': position.getEntryOrder(
        ).getInstrument(), 'stopLoss': ((self.slPercentage/100.0) + 1) * entryPrice,
            'entryOrderId': position.getEntryOrder().getId()})

        return super().onEnterOk(position)

    def onExitOk(self, position: position):
        index = 0
        for openPosition in self.positions.copy():
            if (openPosition['instrument'] == position.getInstrument()) and (openPosition.get('pop', False) == True):
                break
            index += 1

        if index < len(self.positions):
            self.positions.pop(index)

        if self.maxSLCount == self.initialMaxSLCount - 1:
            if self.getCurrentDateTime().time() < self.exitTime:
                self._enterShortStraddle(self.getCurrentDateTime().date())
        else:
            # Set stop losses of remaining positions to self.slPercentage of current price
            for openPosition in self.positions:
                ltp = self.getLTP(openPosition['instrument'])
                openPosition['stopLoss'] = (
                    (self.slPercentage/100.0) + 1) * ltp

        return super().onExitOk(position)

    def _enterShortStraddle(self, currentDate):
        underlyingLTP = self.getLTP(self.underlying)
        if underlyingLTP is None:
            return None

        atmStrike = self.getATMStrike(underlyingLTP, 100)

        currentExpiry = utils.getNearestWeeklyExpiryDate(
            currentDate)

        ceSymbol = self.getOptionSymbol(
            self.underlying, currentExpiry, atmStrike, 'c')
        peSymbol = self.getOptionSymbol(
            self.underlying, currentExpiry, atmStrike, 'p')

        if self.haveLTP(ceSymbol) is None or self.haveLTP(peSymbol) is None:
            return None

        self.log(
            f'Taking a straddle position with {ceSymbol} and {peSymbol} at {self.underlying} LTP of {underlyingLTP}')
        self.state = State.PLACING_ORDERS
        self.enterShort(ceSymbol, self.quantity)
        self.enterShort(peSymbol, self.quantity)

    def closeAllPositions(self):
        if self.state == State.EXITED:
            return

        self.state = State.EXITED
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()
                for openPosition in self.positions:
                    if (position.getInstrument() == openPosition['instrument']) and (position.getEntryOrder().getId() == openPosition['entryOrderId']):
                        openPosition['pop'] = True
                        break

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)

        self.overallPnL = self.getOverallPnL()

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
        # Exit all positions if exit time is met or portfolio SL is hit
        elif bars.getDateTime().time() >= self.exitTime:
            self.closeAllPositions()
        elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            self.log(
                f'Entry time <{self.entryTime}> is greater than current time<{bars.getDateTime()}>.')
            self._enterShortStraddle(bars.getDateTime().date())
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif self.state == State.ENTERED:
            for openPosition in self.positions:
                if self.getLTP(openPosition['instrument']) > openPosition['stopLoss']:
                    if self.maxSLCount > 0:
                        self.log(
                            f"LTP of <{openPosition['instrument']}> has crossed stop loss <{openPosition['stopLoss']}>. Exiting position!")
                        for position in list(self.getActivePositions()):
                            if (position.getInstrument() == openPosition['instrument']) and (position.getEntryOrder().getId() == openPosition['entryOrderId']):
                                self.state = State.PLACING_ORDERS
                                self.maxSLCount -= 1
                                position.exitMarket()
                                openPosition['pop'] = True
                                break
                    else:
                        self.log(
                            'Max SL count has reached. Exiting all positions!')
                        self.closeAllPositions()

        elif self.state == State.EXITED:
            pass


if __name__ == "__main__":
    CliMain(StraddleIntradayV2)
