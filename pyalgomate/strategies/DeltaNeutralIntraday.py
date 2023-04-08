import logging
import datetime
import pandas as pd

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State, Expiry

logger = logging.getLogger(__file__)


class DeltaNeutralIntraday(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, registeredOptionsCount=None, callback=None, resampleFrequency=None, lotSize=None, collectData=None):
        super(DeltaNeutralIntraday, self).__init__(feed, broker,
                                                   strategyName=__class__.__name__,
                                                   logger=logging.getLogger(
                                                       __file__),
                                                   callback=callback,
                                                   resampleFrequency=resampleFrequency,
                                                   collectData=collectData)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.expiry = Expiry.WEEKLY
        self.initialDeltaDifference = 0.2
        self.deltaThreshold = 0.3
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 4000
        self.vegaSL = 1500

        self.registeredOptionsCount = registeredOptionsCount if registeredOptionsCount is not None else 0

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionCall = None
        self.positionPut = None
        self.positionVega = None
        self.numberOfAdjustments = 0

    def closeAllPositions(self):
        self.state = State.EXITED
        if self.positionCall:
            self.positionCall.exitMarket()
            self.positionCall = None

        if self.positionPut:
            self.positionPut.exitMarket()
            self.positionPut = None

        if self.positionVega:
            self.positionVega.exitMarket()
            self.positionVega = None

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)
        overallDelta = self.getOverallDelta()

        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date(
        )) if self.expiry == Expiry.WEEKLY else utils.getNearestMonthlyExpiryDate(bars.getDateTime().date())

        optionData = self.getOptionData(bars)

        if (len(optionData) < self.registeredOptionsCount):
            return

        if self.state == State.LIVE:
            if bars.getDateTime().time() >= self.entryTime and bars.getDateTime().time() < self.exitTime:
                selectedCallOption = self.getNearestDeltaOption(
                    'c', self.initialDeltaDifference, currentExpiry)
                selectedPutOption = self.getNearestDeltaOption(
                    'p', self.initialDeltaDifference, currentExpiry)

                if selectedCallOption is None or selectedPutOption is None:
                    return

                # Return if we do not have LTP for selected options yet
                if not (self.haveLTP(selectedCallOption.optionContract.symbol) and self.haveLTP(selectedPutOption.optionContract.symbol)):
                    return

                # Place initial delta-neutral positions
                self.positionCall = self.enterShort(
                    selectedCallOption.optionContract.symbol, self.quantity)
                self.positionPut = self.enterShort(
                    selectedPutOption.optionContract.symbol, self.quantity)

                self.state = State.PLACING_ORDERS
        elif self.state == State.PLACING_ORDERS:
            # Wait until both positions are entered
            if self.positionCall is not None and self.positionPut is not None:
                if self.positionCall.getInstrument() in self.openPositions and self.positionPut.getInstrument() in self.openPositions:
                    if self.positionVega is not None:
                        if self.positionVega.getInstrument() in self.openPositions:
                            self.state = State.ENTERED
                    else:
                        self.state = State.ENTERED
        elif self.state == State.ENTERED:
            # Exit all positions if exit time is met or portfolio SL is hit
            if bars.getDateTime().time() >= self.exitTime:
                self.closeAllPositions()
                return

            self.overallPnL = self.getOverallPnL()

            if self.overallPnL <= -self.portfolioSL:
                self.log(
                    f"Portfolio SL({self.portfolioSL} is hit. Current PnL is {self.overallPnL}. Exiting all positions!)")
                self.closeAllPositions()
                return

            # Adjust positions if delta difference is more than delta threshold
            callOptionGreeks = optionData[self.positionCall.getInstrument(
            )]
            putOptionGreeks = optionData[self.positionPut.getInstrument()]

            deltaDifference = abs(
                callOptionGreeks.delta + putOptionGreeks.delta)

            if deltaDifference > self.deltaThreshold:
                # Close the profit making position and take another position with delta nearest to that of other option
                if abs(callOptionGreeks.delta) > abs(putOptionGreeks.delta):
                    self.positionPut.exitMarket()
                    # Find put option with delta closest to delta of call option
                    selectedPutOption = self.getNearestDeltaOption(
                        'p', callOptionGreeks.delta, currentExpiry)
                    
                    self.state = State.PLACING_ORDERS
                    self.positionPut = self.enterShort(
                        selectedPutOption.optionContract.symbol, self.quantity)
                else:
                    self.positionCall.exitMarket()
                    # Find call option with delta closest to delta of put option
                    selectedCallOption = self.getNearestDeltaOption(
                        'c', putOptionGreeks.delta, currentExpiry)
                    self.state = State.PLACING_ORDERS
                    self.positionCall = self.enterShort(
                        selectedCallOption.optionContract.symbol, self.quantity)

                self.numberOfAdjustments += 1

            if self.positionVega is None and self.numberOfAdjustments >= 2:
                #monthlyExpiry = utils.getNearestMonthlyExpiryDate(bars.getDateTime().date())
                selectedOption = self.getNearestDeltaOption('c' if abs(
                    callOptionGreeks.delta) > abs(putOptionGreeks.delta) else 'p', 0.5, currentExpiry)
                if selectedOption.optionContract.symbol in [self.positionCall.getInstrument(),
                                                            self.positionPut.getInstrument()]:
                    self.log(
                        f"We just have entered short positon of <{selectedOption.optionContract.symbol}> in current adjustment. Skipping buying same position.")
                else:
                    self.log(
                        f"Number of adjustments has reached {self.numberOfAdjustments}. Managing vega by buying an option. Current PnL is {self.overallPnL}).")
                    self.state = State.PLACING_ORDERS
                    self.positionVega = self.enterLong(
                        selectedOption.optionContract.symbol, self.quantity)
        # Check if we are in the EXITED state
        elif self.state == State.EXITED:
            pass

        self.overallPnL = self.getOverallPnL()

        if bars.getDateTime().time() >= self.marketEndTime:
            if (len(self.openPositions) + len(self.closedPositions)) > 0:
                self.log(
                    f"Overall PnL for {bars.getDateTime().date()} is {self.overallPnL}")
            if self.state != State.LIVE:
                self.__reset__()
