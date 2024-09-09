import asyncio
import datetime
import logging
import threading

import pyalgomate.utils as utils
from pyalgomate.cli import CliMain
from pyalgomate.core import State
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.strategy.position import Position


class NineTwentyStraddle(BaseOptionsGreeksStrategy):
    def __init__(
        self,
        feed,
        broker,
        underlying,
        entryTime=datetime.time(9, 20),
        exitTime=datetime.time(15, 20),
        stopLossPercentage=50,
        lots=1,
        strategyName=None,
        callback=None,
        telegramBot=None,
        telegramChannelId=None,
        telegramMessageThreadId=None,
    ):
        super(NineTwentyStraddle, self).__init__(
            feed,
            broker,
            strategyName=strategyName if strategyName else __class__.__name__,
            logger=logging.getLogger(__name__),
            callback=callback,
            telegramBot=telegramBot,
            telegramChannelId=telegramChannelId,
            telegramMessageThreadId=telegramMessageThreadId,
        )
        self.underlying = underlying
        self.entryTime = entryTime
        self.exitTime = exitTime
        self.stopLossPercentage = stopLossPercentage
        self.lots = lots
        self.cePosition: Position = None
        self.pePosition: Position = None

        underlyingDetails = self.getBroker().getUnderlyingDetails(self.underlying)
        self.underlyingIndex = underlyingDetails["index"]
        self.strikeDifference = underlyingDetails["strikeDifference"]
        self.lotSize = underlyingDetails["lotSize"]

        self.quantity = self.lotSize * self.lots

    def onBars(self, bars):
        currentDateTime = bars.getDateTime()
        currentTime = currentDateTime.time()

        if self.state == State.LIVE:
            if currentTime >= self.entryTime:
                entryComplete = threading.Event()

                def onEntryComplete(result):
                    entryComplete.set()

                self.runAsync(
                    self.enterShortStraddle(currentDateTime.date()), onEntryComplete
                )

                entryComplete.wait()
        elif self.state == State.ENTERED:
            if currentTime >= self.exitTime:
                self.log("Exit time reached. Closing all positions.")
                self.closeAllPositions()

    async def enterShortStraddle(self, currentDate):
        underlyingLTP = self.getLTP(self.underlying)
        if underlyingLTP is None:
            self.log(f"Unable to fetch LTP for {self.underlying}")
            return

        atmStrike = self.getATMStrike(underlyingLTP, self.strikeDifference)
        currentExpiry = utils.getNearestWeeklyExpiryDate(
            currentDate, self.underlyingIndex
        )

        ceSymbol = self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, "c")
        peSymbol = self.getOptionSymbol(self.underlying, currentExpiry, atmStrike, "p")

        # Enter short straddle asynchronously
        self.cePosition, self.pePosition = await asyncio.gather(
            self.enterShortLimitAsync(
                ceSymbol, self.getLastPrice(ceSymbol), self.quantity
            ),
            self.enterShortLimitAsync(
                peSymbol, self.getLastPrice(ceSymbol), self.quantity
            ),
        )

        if self.cePosition and self.pePosition:
            self.state = State.ENTERED
            self.log(f"Entered short straddle: CE {ceSymbol}, PE {peSymbol}")

    def onEnterOk(self, position: Position):
        super().onEnterOk(position)
        entry_price = position.getEntryOrder().getAvgFillPrice()
        stop_loss = entry_price * (1 + self.stopLossPercentage / 100)
        self.runAsync(self.setStopLoss(position, stop_loss))
        self.log(f"Set SL for {position.getInstrument()} at {stop_loss:.2f}")

    def onExitOk(self, position: Position):
        super().onExitOk(position)
        self.log(f"Position closed for {position.getInstrument()}")

        if position == self.cePosition:
            self.cePosition = None
        elif position == self.pePosition:
            self.pePosition = None

        # Check if the other leg is still active
        other_position = self.cePosition if self.pePosition is None else self.pePosition
        if other_position and other_position.exitActive():
            entry_price = other_position.getEntryOrder().getAvgFillPrice()
            self.runAsync(self.setStopLoss(other_position, entry_price))
            self.log(f"Brought SL to cost for {other_position.getInstrument()}")

        if self.cePosition is None and self.pePosition is None:
            self.state = State.EXITED
            self.log("All positions closed")

    async def setStopLoss(self, position: Position, stop_loss):
        if position.exitActive():
            await position.modifyExitStopLimit(stop_loss, stop_loss)
        else:
            await position.exitStopLimit(stop_loss, stop_loss)

    async def closePositionsAsync(self, positions):
        close_tasks = []
        for position in positions:
            if position and position.isOpen():
                close_tasks.append(self.exitWithMarketProtection(position))
        await asyncio.gather(*close_tasks)

    def closeAllPositions(self):
        self.state = State.SQUARING_OFF
        positions = [
            pos for pos in [self.cePosition, self.pePosition] if pos and pos.isOpen()
        ]
        self.runAsync(self.closePositionsAsync(positions))

    async def exitWithMarketProtection(self, position):
        lastBar = self.getFeed().getLastBar(position.getInstrument())
        if lastBar is None:
            self.log(
                f"LTP of <{position.getInstrument()}> is None while exiting with market protection."
            )
            return

        limitPrice = lastBar.getClose()
        if position.getEntryOrder().isBuy():
            limitPrice *= 0.95  # 5% below LTP for long positions
        else:
            limitPrice *= 1.05  # 5% above LTP for short positions

        if position.exitActive():
            await position.modifyExitToLimit(limitPrice)
        else:
            await position.exitLimit(limitPrice)

    def onStart(self):
        super().onStart()
        self.state = State.LIVE

    def onFinish(self, bars):
        super().onFinish(bars)
        self.log("Finished")


if __name__ == "__main__":
    CliMain(NineTwentyStraddle)
