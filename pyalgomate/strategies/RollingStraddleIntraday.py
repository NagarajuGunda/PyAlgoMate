import logging
import datetime

import pyalgomate.utils as utils
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.strategies.BaseOptionsGreeksStrategy import State, Expiry


class RollingStraddleIntraday(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker, registeredOptionsCount=None, callback=None, resampleFrequency=None, lotSize=None):
        super(RollingStraddleIntraday, self).__init__(feed, broker,
                                                      strategyName=__class__.__name__,
                                                      logger=logging.getLogger(
                                                          __file__),
                                                      callback=callback, resampleFrequency=resampleFrequency)

        self.entryTime = datetime.time(hour=9, minute=17)
        self.exitTime = datetime.time(hour=15, minute=15)
        self.expiry = Expiry.WEEKLY
        self.initialDeltaDifference = 0.5
        self.strikeThreshold = 100
        self.lotSize = lotSize if lotSize is not None else 25
        self.lots = 1
        self.quantity = self.lotSize * self.lots
        self.portfolioSL = 4000

        self.registeredOptionsCount = registeredOptionsCount if registeredOptionsCount is not None else 0

        self.__reset__()

    def __reset__(self):
        super().reset()
        # members that needs to be reset after exit time
        self.positionCall = None
        self.positionPut = None
        self.atmStrike = None
        self.optionData = {}
        self.numberOfAdjustments = 0

    def closeAllPositions(self):
        self.state = State.EXITED
        if self.positionCall:
            self.positionCall.exitMarket()
            self.positionCall = None

        if self.positionPut:
            self.positionPut.exitMarket()
            self.positionPut = None

    def canTakePositions(self, expiry):
        selectedCallOption = self.getNearestDeltaOption(
            'c', self.initialDeltaDifference, expiry)
        selectedPutOption = self.getNearestDeltaOption(
            'p', self.initialDeltaDifference, expiry)

        if selectedCallOption is None or selectedPutOption is None:
            return None

        # Return if we do not have LTP for selected options yet
        if not (self.haveLTP(selectedCallOption.optionContract.symbol) and self.haveLTP(selectedPutOption.optionContract.symbol)):
            return None

        return [selectedCallOption.optionContract, selectedPutOption.optionContract]

    def takePositions(self, currentExpiry, currentDateTime):
        if self.positionCall is not None or self.positionPut is not None:
            return

        positions = self.canTakePositions(currentExpiry)
        if positions is not None:
            self.state = State.PLACING_ORDERS
            # Place initial delta-neutral positions
            self.positionCall = self.enterShort(
                positions[0].symbol, self.quantity)
            self.positionPut = self.enterShort(
                positions[1].symbol, self.quantity)
            self.atmStrike = positions[0].strike
            self.log(
                f"Date/Time {currentDateTime}. Taking positions at strike {self.atmStrike}")

    def canExitAllPositions(self, currentTime):
        # Exit all positions if exit time is met or portfolio SL is hit
        if currentTime >= self.exitTime:
            self.log(
                f"Current time {currentTime} is >= Exit time {self.exitTime}. Closing all positions!")
            return True

        self.overallPnL = self.getOverallPnL()

        if self.overallPnL <= -self.portfolioSL:
            self.log(
                f"Portfolio SL({self.portfolioSL} is hit. Current PnL is {self.overallPnL}. Exiting all positions!)")
            return True

        return False

    def canDoAdjustments(self):
        if self.positionCall is not None and self.positionPut is not None:
            # Roll the straddle to a new strike
            optionContract = self.getBroker().getOptionContract(
                self.positionCall.getInstrument())
            underlyingPrice = self.getUnderlyingPrice(
                optionContract.underlying)

            if abs(underlyingPrice - self.atmStrike) > self.strikeThreshold:
                self.log(
                    f"Underlying price {underlyingPrice} has exceeded strike threshold {self.strikeThreshold}. Current positions are at strike {self.atmStrike}")
                return True

        return False

        # if self.positionCall is not None and self.positionPut is not None:
        #     callOptionGreeks = self.optionData[self.positionCall.getInstrument(
        #     )]
        #     putOptionGreeks = self.optionData[self.positionPut.getInstrument(
        #     )]

        #     deltaDifference = abs(
        #         callOptionGreeks.delta + putOptionGreeks.delta)

        #     deltaThreshold = 0.2
        #     if deltaDifference > deltaThreshold:
        #         self.log(
        #             f"Overall delta {deltaDifference} has breached delta threshold {deltaThreshold}. Adjustments needed")
        #         return True
        # return False

    def onBars(self, bars):
        self.log(f"Bar date times - {bars.getDateTime()}", logging.DEBUG)
        overallDelta = self.getOverallDelta()

        currentExpiry = utils.getNearestWeeklyExpiryDate(bars.getDateTime().date(
        )) if self.expiry == Expiry.WEEKLY else utils.getNearestMonthlyExpiryDate(bars.getDateTime().date())

        # Skip the monthly expiry days
        if utils.getNearestMonthlyExpiryDate(bars.getDateTime().date()) == bars.getDateTime().date():
            return

        # set exit time based on expiry/non-expiry day
        if currentExpiry == bars.getDateTime().date():
            self.exitTime = datetime.time(hour=15, minute=25)
        else:
            datetime.time(hour=14, minute=00)

        self.optionData = self.getOptionData(bars)
        if (self.registeredOptionsCount > 0) and (len(self.optionData) < self.registeredOptionsCount):
            return

        if self.state == State.LIVE:
            if bars.getDateTime().time() >= self.entryTime and bars.getDateTime().time() < self.exitTime:
                self.takePositions(currentExpiry, bars.getDateTime())
        elif self.state == State.PLACING_ORDERS:
            # Wait until both positions are entered
            if self.positionCall is not None and self.positionPut is not None:
                if self.positionCall.getInstrument() in self.openPositions and self.positionPut.getInstrument() in self.openPositions:
                    self.state = State.ENTERED
        elif self.state == State.ENTERED:
            if self.canExitAllPositions(bars.getDateTime().time()):
                self.closeAllPositions()
            elif self.canDoAdjustments():
                self.closeAllPositions()
                self.takePositions(currentExpiry, bars.getDateTime())
                self.numberOfAdjustments += 1
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
