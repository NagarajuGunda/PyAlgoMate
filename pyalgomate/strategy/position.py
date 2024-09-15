import datetime

from pyalgotrade import warninghelpers
from pyalgomate.core import broker
from pyalgotrade.stratanalyzer import returns


class PositionState(object):

    def onEnter(self, position):
        pass

    # Raise an exception if an order can't be submitted in the current state.
    def canSubmitOrder(self, position, order):
        raise NotImplementedError()

    def onOrderEvent(self, position, orderEvent):
        raise NotImplementedError()

    def isOpen(self, position):
        raise NotImplementedError()

    async def exit(
        self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None
    ):
        raise NotImplementedError()


class WaitingEntryState(PositionState):

    def canSubmitOrder(self, position, order):
        if position.entryActive():
            raise Exception("The entry order is still active")

    def onOrderEvent(self, position, orderEvent):
        assert position.getEntryOrder().getId() == orderEvent.getOrder().getId()

        if orderEvent.getEventType() in (
            broker.OrderEvent.Type.FILLED,
            broker.OrderEvent.Type.PARTIALLY_FILLED,
        ):
            position.switchState(OpenState())
            position.getStrategy().onEnterOk(position)
        elif orderEvent.getEventType() == broker.OrderEvent.Type.CANCELED:
            assert position.getEntryOrder().getFilled() == 0
            position.switchState(ClosedState())
            position.getStrategy().onEnterCanceled(position)

    def isOpen(self, position):
        return True

    async def exit(
        self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None
    ):
        assert position.getShares() == 0
        assert position.getEntryOrder().isActive()
        await position.getStrategy().getBroker().cancelOrder(position.getEntryOrder())


class OpenState(PositionState):

    def onEnter(self, position):
        entryDateTime = position.getEntryOrder().getExecutionInfo().getDateTime()
        position.setEntryDateTime(entryDateTime)

    def canSubmitOrder(self, position, order):
        # Only exit orders should be submitted in this state.
        pass

    def onOrderEvent(self, position, orderEvent):
        if (
            position.getExitOrder()
            and position.getExitOrder().getId() == orderEvent.getOrder().getId()
        ):
            if orderEvent.getEventType() == broker.OrderEvent.Type.FILLED:
                if position.getShares() == 0:
                    position.switchState(ClosedState())
                    position.getStrategy().onExitOk(position)
            elif orderEvent.getEventType() == broker.OrderEvent.Type.CANCELED:
                assert position.getShares() != 0
                position.getStrategy().onExitCanceled(position)
        elif position.getEntryOrder().getId() == orderEvent.getOrder().getId():
            # Nothing to do since the entry order may be completely filled or canceled after a partial fill.
            assert position.getShares() != 0
        elif orderEvent.getEventType() == broker.OrderEvent.Type.ACCEPTED:
            pass
        else:
            raise Exception(
                "Invalid order event '%s' in OpenState" % (orderEvent.getEventType())
            )

    def isOpen(self, position):
        return True

    async def exit(
        self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None
    ):
        assert position.getShares() != 0

        # Fail if a previous exit order is active.
        if position.exitActive():
            raise Exception("Exit order is active and it should be canceled first")

        # If the entry order is active, request cancellation.
        if position.entryActive():
            await position.getStrategy().getBroker().cancelOrder(
                position.getEntryOrder()
            )

        await position._submitExitOrder(stopPrice, limitPrice, goodTillCanceled)


class ClosedState(PositionState):

    def onEnter(self, position):
        # Set the exit datetime if the exit order was filled.
        if position.exitFilled():
            exitDateTime = position.getExitOrder().getExecutionInfo().getDateTime()
            position.setExitDateTime(exitDateTime)

        assert position.getShares() == 0
        position.getStrategy().unregisterPosition(position)

    def canSubmitOrder(self, position, order):
        raise Exception("The position is closed")

    def onOrderEvent(self, position, orderEvent):
        raise Exception(
            "Invalid order event '%s' in ClosedState" % (orderEvent.getEventType())
        )

    def isOpen(self, position):
        return False

    async def exit(
        self, position, stopPrice=None, limitPrice=None, goodTillCanceled=None
    ):
        pass


class Position(object):

    def __init__(self):
        self.__state: PositionState = None
        self.__activeOrders = set()
        self.__shares = 0
        self.__strategy = None
        self.__entryOrder = None
        self.__entryDateTime = None
        self.__exitOrder = None
        self.__exitDateTime = None
        self.__posTracker = None
        self.__allOrNone = None
        self.__customAttributes = None  # Store custom attributes

        self.switchState(WaitingEntryState())

    async def build(self, strategy, entryOrder, goodTillCanceled, allOrNone, **kwargs):
        # The order must be created but not submitted.
        assert entryOrder.isInitial()

        self.__state: PositionState = None
        self.__activeOrders = set()
        self.__shares = 0
        self.__strategy = strategy
        self.__entryOrder: broker.Order = None
        self.__entryDateTime = None
        self.__exitOrder: broker.Order = None
        self.__exitDateTime = None
        self.__posTracker = returns.PositionTracker(entryOrder.getInstrumentTraits())
        self.__allOrNone = allOrNone
        self.__customAttributes = kwargs  # Store custom attributes

        self.switchState(WaitingEntryState())

        entryOrder.setGoodTillCanceled(goodTillCanceled)
        entryOrder.setAllOrNone(allOrNone)
        await self.__submitAndRegisterOrder(entryOrder)
        self.__entryOrder = entryOrder

        return self

    async def __submitAndRegisterOrder(self, order):
        assert order.isInitial()

        # Check if an order can be submitted in the current state.
        self.__state.canSubmitOrder(self, order)

        # This may raise an exception, so we wan't to submit the order before moving forward and registering
        # the order in the strategy.
        await self.getStrategy().getBroker().submitOrder(order)

        self.__activeOrders.add(order)
        self.getStrategy().registerPositionOrder(self, order)

    async def __modifyAndRegisterOrder(self, oldOrder, newOrder):
        # Check if an order can be submitted in the current state.
        self.__state.canSubmitOrder(self, newOrder)

        # This may raise an exception, so we wan't to submit the order before moving forward and registering
        # the order in the strategy.
        await self.getStrategy().getBroker().modifyOrder(oldOrder, newOrder)

        self.__activeOrders.discard(oldOrder)
        self.__activeOrders.add(newOrder)

        self.getStrategy().unregisterPositionOrder(self, oldOrder)
        self.getStrategy().registerPositionOrder(self, newOrder)

    def setEntryDateTime(self, dateTime):
        self.__entryDateTime = dateTime

    def setExitDateTime(self, dateTime):
        self.__exitDateTime = dateTime

    def switchState(self, newState):
        self.__state = newState
        self.__state.onEnter(self)

    def getStrategy(self):
        return self.__strategy

    def getLastPrice(self):
        return self.__strategy.getLastPrice(self.getInstrument())

    def getActiveOrders(self):
        return list(self.__activeOrders)

    def getShares(self):
        """Returns the number of shares.
        This will be a possitive number for a long position, and a negative number for a short position.

        .. note::
            If the entry order was not filled, or if the position is closed, then the number of shares will be 0.
        """
        return self.__shares

    def entryActive(self):
        """Returns True if the entry order is active."""
        return self.__entryOrder is not None and self.__entryOrder.isActive()

    def entryFilled(self):
        """Returns True if the entry order was filled."""
        return self.__entryOrder is not None and self.__entryOrder.isFilled()

    def exitActive(self):
        """Returns True if the exit order is active."""
        return self.__exitOrder is not None and self.__exitOrder.isActive()

    def exitFilled(self):
        """Returns True if the exit order was filled."""
        return self.__exitOrder is not None and self.__exitOrder.isFilled()

    def getEntryOrder(self):
        """Returns the :class:`pyalgotrade.broker.Order` used to enter the position."""
        return self.__entryOrder

    def getExitOrder(self):
        """Returns the :class:`pyalgotrade.broker.Order` used to exit the position. If this position hasn't been closed yet, None is returned."""
        return self.__exitOrder

    def getInstrument(self):
        """Returns the instrument used for this position."""
        return self.__entryOrder.getInstrument()

    async def getReturn(self, includeCommissions=True):
        """
        Calculates cumulative percentage returns up to this point.
        If the position is not closed, these will be unrealized returns.
        """

        # Deprecated in v0.18.
        if includeCommissions is False:
            warninghelpers.deprecation_warning(
                "includeCommissions will be deprecated in the next version.",
                stacklevel=2,
            )

        ret = 0
        price = self.getLastPrice()
        if price is not None:
            ret = self.__posTracker.getReturn(price, includeCommissions)
        return ret

    def getPnL(self, includeCommissions=True):
        """
        Calculates PnL up to this point.
        If the position is not closed, these will be unrealized PnL.
        """

        # Deprecated in v0.18.
        if includeCommissions is False:
            warninghelpers.deprecation_warning(
                "includeCommissions will be deprecated in the next version.",
                stacklevel=2,
            )

        ret = 0
        price = self.getLastPrice()
        if price is not None:
            ret = self.__posTracker.getPnL(
                price=price, includeCommissions=includeCommissions
            )
        return ret

    async def cancelEntry(self):
        """Cancels the entry order if its active."""
        if self.entryActive():
            await self.getStrategy().getBroker().cancelOrder(self.getEntryOrder())

    async def cancelExit(self):
        """Cancels the exit order if its active."""
        if self.exitActive():
            await self.getStrategy().getBroker().cancelOrder(self.getExitOrder())

    async def exitMarket(self, goodTillCanceled=None):
        """Submits a market order to close this position.

        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        await self.__state.exit(self, None, None, goodTillCanceled)

    async def exitLimit(self, limitPrice, goodTillCanceled=None):
        """Submits a limit order to close this position.

        :param limitPrice: The limit price.
        :type limitPrice: float.
        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        await self.__state.exit(self, None, limitPrice, goodTillCanceled)

    async def exitStop(self, stopPrice, goodTillCanceled=None):
        """Submits a stop order to close this position.

        :param stopPrice: The stop price.
        :type stopPrice: float.
        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        await self.__state.exit(self, stopPrice, None, goodTillCanceled)

    async def exitStopLimit(self, stopPrice, limitPrice, goodTillCanceled=None):
        """Submits a stop limit order to close this position.

        :param stopPrice: The stop price.
        :type stopPrice: float.
        :param limitPrice: The limit price.
        :type limitPrice: float.
        :param goodTillCanceled: True if the exit order is good till canceled. If False then the order gets automatically canceled when the session closes. If None, then it will match the entry order.
        :type goodTillCanceled: boolean.

        .. note::
            * If the position is closed (entry canceled or exit filled) this won't have any effect.
            * If the exit order for this position is pending, an exception will be raised. The exit order should be canceled first.
            * If the entry order is active, cancellation will be requested.
        """

        await self.__state.exit(self, stopPrice, limitPrice, goodTillCanceled)

    async def modifyExitToLimit(self, limitPrice, goodTillCanceled=None):
        """Modifies the exit order to a limit order."""
        assert self.exitActive()

        exitOrder: broker.Order = await self.buildExitOrder(None, limitPrice)

        # If goodTillCanceled was not set, match the entry order.
        if goodTillCanceled is None:
            goodTillCanceled = self.__entryOrder.getGoodTillCanceled()
        exitOrder.setGoodTillCanceled(goodTillCanceled)

        exitOrder.setAllOrNone(self.__allOrNone)

        currentOrder = self.__exitOrder
        self.__exitOrder = exitOrder
        await self.__modifyAndRegisterOrder(currentOrder, self.__exitOrder)

    async def modifyExitStopLimit(self, stopPrice, limitPrice, goodTillCanceled=None):
        """Modifies the exit order to a limit order."""
        assert self.exitActive()

        exitOrder: broker.Order = await self.buildExitOrder(stopPrice, limitPrice)

        # If goodTillCanceled was not set, match the entry order.
        if goodTillCanceled is None:
            goodTillCanceled = self.__entryOrder.getGoodTillCanceled()
        exitOrder.setGoodTillCanceled(goodTillCanceled)

        exitOrder.setAllOrNone(self.__allOrNone)

        currentOrder = self.__exitOrder
        self.__exitOrder = exitOrder
        await self.__modifyAndRegisterOrder(currentOrder, self.__exitOrder)

    async def _submitExitOrder(self, stopPrice, limitPrice, goodTillCanceled):
        assert not self.exitActive()

        exitOrder = await self.buildExitOrder(stopPrice, limitPrice)

        # If goodTillCanceled was not set, match the entry order.
        if goodTillCanceled is None:
            goodTillCanceled = self.__entryOrder.getGoodTillCanceled()
        exitOrder.setGoodTillCanceled(goodTillCanceled)

        exitOrder.setAllOrNone(self.__allOrNone)

        await self.__submitAndRegisterOrder(exitOrder)
        self.__exitOrder = exitOrder

    def onOrderEvent(self, orderEvent):
        self.__updatePosTracker(orderEvent)

        order = orderEvent.getOrder()
        if not order.isActive():
            self.__activeOrders.discard(order)

        # Update the number of shares.
        if orderEvent.getEventType() in (
            broker.OrderEvent.Type.PARTIALLY_FILLED,
            broker.OrderEvent.Type.FILLED,
        ):
            execInfo = orderEvent.getEventInfo()
            # roundQuantity is used to prevent bugs like the one triggered in testcases.bitstamp_test:TestCase.testRoundingBug
            if order.isBuy():
                self.__shares = order.getInstrumentTraits().roundQuantity(
                    self.__shares + execInfo.getQuantity()
                )
            else:
                self.__shares = order.getInstrumentTraits().roundQuantity(
                    self.__shares - execInfo.getQuantity()
                )

        self.__state.onOrderEvent(self, orderEvent)

    def __updatePosTracker(self, orderEvent):
        if orderEvent.getEventType() in (
            broker.OrderEvent.Type.PARTIALLY_FILLED,
            broker.OrderEvent.Type.FILLED,
        ):
            order = orderEvent.getOrder()
            execInfo = orderEvent.getEventInfo()
            if order.isBuy():
                self.__posTracker.buy(
                    execInfo.getQuantity(),
                    execInfo.getPrice(),
                    execInfo.getCommission(),
                )
            else:
                self.__posTracker.sell(
                    execInfo.getQuantity(),
                    execInfo.getPrice(),
                    execInfo.getCommission(),
                )

    async def buildExitOrder(self, stopPrice, limitPrice):
        raise NotImplementedError()

    def isOpen(self):
        """Returns True if the position is open."""
        return self.__state.isOpen(self)

    async def getAge(self):
        """Returns the duration in open state.

        :rtype: datetime.timedelta.

        .. note::
            * If the position is open, then the difference between the entry datetime and the datetime of the last bar is returned.
            * If the position is closed, then the difference between the entry datetime and the exit datetime is returned.
        """
        ret = datetime.timedelta()
        if self.__entryDateTime is not None:
            if self.__exitDateTime is not None:
                last = self.__exitDateTime
            else:
                last = self.__strategy.getCurrentDateTime()
            ret = last - self.__entryDateTime
        return ret

    def setCustomAttribute(self, key, value):
        """Set a custom attribute for the position."""
        self.__customAttributes[key] = value

    def getCustomAttribute(self, key, default=None):
        """Get a custom attribute for the position."""
        return self.__customAttributes.get(key, default)

    def getAllCustomAttributes(self):
        """Get all custom attributes for the position."""
        return self.__customAttributes.copy()


# This class is reponsible for order management in long positions.
class LongPosition(Position):

    def __init__(self):
        super(LongPosition, self).__init__()

    async def build(
        self,
        strategy,
        instrument,
        stopPrice,
        limitPrice,
        quantity,
        goodTillCanceled,
        allOrNone,
        **kwargs
    ):
        if limitPrice is None and stopPrice is None:
            entryOrder = strategy.getBroker().createMarketOrder(
                broker.Order.Action.BUY, instrument, quantity, False
            )
        elif limitPrice is not None and stopPrice is None:
            entryOrder = strategy.getBroker().createLimitOrder(
                broker.Order.Action.BUY, instrument, limitPrice, quantity
            )
        elif limitPrice is None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopOrder(
                broker.Order.Action.BUY, instrument, stopPrice, quantity
            )
        elif limitPrice is not None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopLimitOrder(
                broker.Order.Action.BUY, instrument, stopPrice, limitPrice, quantity
            )
        else:
            assert False

        return await super(LongPosition, self).build(
            strategy, entryOrder, goodTillCanceled, allOrNone, **kwargs
        )

    async def buildExitOrder(self, stopPrice, limitPrice):
        quantity = self.getShares()
        assert quantity > 0
        if limitPrice is None and stopPrice is None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createMarketOrder(
                    broker.Order.Action.SELL, self.getInstrument(), quantity, False
                )
            )
        elif limitPrice is not None and stopPrice is None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createLimitOrder(
                    broker.Order.Action.SELL, self.getInstrument(), limitPrice, quantity
                )
            )
        elif limitPrice is None and stopPrice is not None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createStopOrder(
                    broker.Order.Action.SELL, self.getInstrument(), stopPrice, quantity
                )
            )
        elif limitPrice is not None and stopPrice is not None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createStopLimitOrder(
                    broker.Order.Action.SELL,
                    self.getInstrument(),
                    stopPrice,
                    limitPrice,
                    quantity,
                )
            )
        else:
            assert False

        return ret


# This class is reponsible for order management in short positions.
class ShortPosition(Position):

    def __init__(self):
        super(ShortPosition, self).__init__()

    async def build(
        self,
        strategy,
        instrument,
        stopPrice,
        limitPrice,
        quantity,
        goodTillCanceled,
        allOrNone,
        **kwargs
    ):
        if limitPrice is None and stopPrice is None:
            entryOrder = strategy.getBroker().createMarketOrder(
                broker.Order.Action.SELL_SHORT, instrument, quantity, False
            )
        elif limitPrice is not None and stopPrice is None:
            entryOrder = strategy.getBroker().createLimitOrder(
                broker.Order.Action.SELL_SHORT, instrument, limitPrice, quantity
            )
        elif limitPrice is None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopOrder(
                broker.Order.Action.SELL_SHORT, instrument, stopPrice, quantity
            )
        elif limitPrice is not None and stopPrice is not None:
            entryOrder = strategy.getBroker().createStopLimitOrder(
                broker.Order.Action.SELL_SHORT,
                instrument,
                stopPrice,
                limitPrice,
                quantity,
            )
        else:
            assert False

        return await super(ShortPosition, self).build(
            strategy, entryOrder, goodTillCanceled, allOrNone, **kwargs
        )

    async def buildExitOrder(self, stopPrice, limitPrice):
        quantity = self.getShares() * -1
        assert quantity > 0
        if limitPrice is None and stopPrice is None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createMarketOrder(
                    broker.Order.Action.BUY_TO_COVER,
                    self.getInstrument(),
                    quantity,
                    False,
                )
            )
        elif limitPrice is not None and stopPrice is None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createLimitOrder(
                    broker.Order.Action.BUY_TO_COVER,
                    self.getInstrument(),
                    limitPrice,
                    quantity,
                )
            )
        elif limitPrice is None and stopPrice is not None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createStopOrder(
                    broker.Order.Action.BUY_TO_COVER,
                    self.getInstrument(),
                    stopPrice,
                    quantity,
                )
            )
        elif limitPrice is not None and stopPrice is not None:
            ret = (
                self.getStrategy()
                .getBroker()
                .createStopLimitOrder(
                    broker.Order.Action.BUY_TO_COVER,
                    self.getInstrument(),
                    stopPrice,
                    limitPrice,
                    quantity,
                )
            )
        else:
            assert False

        return ret
