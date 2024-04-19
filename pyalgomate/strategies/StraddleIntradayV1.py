import logging
import datetime

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State, Expiry
from pyalgomate.cli import CliMain


class StraddleIntradayV1(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, underlying=None, strategyName=None, registeredOptionsCount=None,
                 callback=None, lotSize=None, collectData=None, telegramBot=None):
        super(StraddleIntradayV1, self).__init__(feed, broker,
                                                 strategyName=strategyName if strategyName else __class__.__name__,
                                                 logger=logging.getLogger(
                                                     __file__),
                                                 callback=callback,
                                                 collectData=collectData,
                                                 telegramBot=telegramBot)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.expiry = Expiry.WEEKLY
        self.initialDeltaDifference = 0.5
        self.deltaThreshold = 0.3
        self.buyDelta = 0.6
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 4000
        self.buySL = 25

        self.registeredOptionsCount = registeredOptionsCount if registeredOptionsCount is not None else 0

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionCall = None
        self.positionPut = None
        self.positionBuy = None

    def closeAllPositions(self):
        if self.state == State.EXITED:
            return

        self.state = State.EXITED
        for position in list(self.getActivePositions()):
            if not position.exitActive():
                position.exitMarket()
        self.positionCall = self.positionPut = self.positionBuy = None

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)
        overallDelta = self.getOverallDelta()

        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date(
        )) if self.expiry == Expiry.WEEKLY else utils.getNearestMonthlyExpiryDate(bars.getDateTime().date())

        optionData = self.getOptionData(bars)
        if (self.registeredOptionsCount > 0) and (len(optionData) < self.registeredOptionsCount):
            return

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
                    f'Current time <{bars.getDateTime().time()}> has crossed exit time <{self.exitTime}. Closing all '
                    f'positions!')
                self.closeAllPositions()
        elif self.overallPnL <= -self.portfolioSL:
            if self.state != State.EXITED:
                self.log(
                    f'Current PnL <{self.overallPnL}> has crossed potfolio SL <{self.portfolioSL}>. Closing all '
                    f'positions!')
                self.closeAllPositions()
        elif (self.state == State.LIVE) and (self.entryTime <= bars.getDateTime().time() < self.exitTime):
            selectedCallOption = self.getNearestDeltaOption(
                'c', self.initialDeltaDifference, currentExpiry)
            selectedPutOption = self.getNearestDeltaOption(
                'p', self.initialDeltaDifference, currentExpiry)

            if selectedCallOption is None or selectedPutOption is None:
                return

            # Return if we do not have LTP for selected options yet
            if not (self.haveLTP(selectedCallOption.optionContract.symbol) and self.haveLTP(
                    selectedPutOption.optionContract.symbol)):
                return

            # Place initial delta-neutral positions
            self.positionCall = self.enterShort(
                selectedCallOption.optionContract.symbol, self.quantity)
            self.positionPut = self.enterShort(
                selectedPutOption.optionContract.symbol, self.quantity)
            self.log(
                f"Date/Time {bars.getDateTime()}. Taking initial positions!")
            self.state = State.PLACING_ORDERS
        elif self.state == State.PLACING_ORDERS:
            if len(list(self.getActivePositions())) == 0:
                self.state = State.LIVE
                return
            if self.isPendingOrdersCompleted():
                self.state = State.ENTERED
                return
        elif self.state == State.ENTERED:
            if self.positionCall is not None and self.positionPut is not None:
                # Cut off position if threshold has reached and move SL to cost for opposite position
                callOptionGreeks = optionData[self.positionCall.getInstrument(
                )]
                putOptionGreeks = optionData[self.positionPut.getInstrument(
                )]

                deltaDifference = abs(
                    callOptionGreeks.delta + putOptionGreeks.delta)

                if deltaDifference > self.deltaThreshold:
                    if abs(
                            callOptionGreeks.delta) > abs(putOptionGreeks.delta):
                        self.log(
                            f'Call delta <{callOptionGreeks.delta}> plus put delta <{putOptionGreeks.delta}> is '
                            f'higher threshold <{self.deltaThreshold}>. Current PNL is <{self.overallPnL}>. Closing '
                            f'call option and moving SL to cost for put option')
                        self.state = State.PLACING_ORDERS
                        self.positionCall.exitMarket()
                        self.positionCall = None

                        buySymbol = self.getNearestDeltaOption(
                            'c', self.buyDelta, currentExpiry).optionContract.symbol
                        self.log(f'Entering long for {buySymbol}')
                        self.positionBuy = self.enterLong(
                            buySymbol, self.quantity)
                    else:
                        self.log(
                            f'Call delta <{callOptionGreeks.delta}> plus put delta <{putOptionGreeks.delta}> is '
                            f'higher threshold <{self.deltaThreshold}>. Current PNL is <{self.overallPnL}>. Closing '
                            f'put option and moving SL to cost for call option')
                        self.state = State.PLACING_ORDERS
                        self.positionPut.exitMarket()
                        self.positionPut = None

                        buySymbol = self.getNearestDeltaOption(
                            'p', self.buyDelta, currentExpiry).optionContract.symbol
                        self.log(f'Entering long for {buySymbol}')
                        self.positionBuy = self.enterLong(
                            buySymbol, self.quantity)
                    return
            else:
                if self.positionBuy:
                    # Check if SL is hit for the buy position
                    entryOrder = self.openPositions[self.positionBuy.getInstrument(
                    )]
                    pnl = self.getPnL(entryOrder)
                    entryPrice = entryOrder.getAvgFillPrice()

                    pnLPercentage = (
                                            pnl / (entryPrice * self.quantity)) * 100

                    if pnLPercentage <= -self.buySL:
                        self.log(
                            f'SL {self.buySL}% hit for {self.positionBuy.getInstrument()}. Current PnL <{pnl}> and percentage <{pnLPercentage}>. Exiting position!')
                        self.state = State.PLACING_ORDERS
                        self.positionBuy.exitMarket()
                        self.positionBuy = None

                position = self.positionCall if self.positionCall is not None else self.positionPut
                if position is not None:
                    entryOrder = self.openPositions[position.getInstrument()]
                    ltp = self.getLTP(entryOrder)
                    entryPrice = entryOrder.getAvgFillPrice()
                    if ltp > entryPrice:
                        self.log(
                            f'LTP of {position.getInstrument()} has crossed SL <{entryPrice}>. Exiting position!')
                        self.state = State.PLACING_ORDERS
                        position.exitMarket()
                        self.positionCall = self.positionPut = None
                        return
        # Check if we are in the EXITED state
        elif self.state == State.EXITED:
            pass


if __name__ == "__main__":
    CliMain(StraddleIntradayV1)
