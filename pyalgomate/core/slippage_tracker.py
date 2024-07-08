import csv
from datetime import datetime
from typing import Dict, List
from pyalgotrade.broker import Order


class SlippageTracker:
    def __init__(self, filename: str):
        self.filename = filename
        self.orders: Dict[Order, float] = {}
        self.fieldnames = ['Order ID', 'Instrument', 'Order Type', 'Action', 'Quantity',
                           'Market Price', 'Limit Price', 'Trigger Price', 'Fill Price', 'Slippage', 'Date/Time']

    def recordOrder(self, order: Order, lastPrice: float):
        self.orders[order] = lastPrice

    def recordFill(self, order: Order, fillPrice: float, dateTime: datetime):
        if order in self.orders:
            marketPrice = self.orders[order]
            limitPrice = None
            triggerPrice = None
            slippage = None
            if order.getType() == Order.Type.MARKET:
                slippage = self.orders[order] - fillPrice
            elif order.getType() == Order.Type.LIMIT:
                limitPrice = order.getLimitPrice()
                slippage = limitPrice - fillPrice
            elif order.getType() in [Order.Type.STOP, Order.Type.STOP_LIMIT]:
                triggerPrice = order.getStopPrice()
                slippage = triggerPrice - fillPrice

            self._writeToFile({
                'Order ID': order.getId(),
                'Instrument': order.getInstrument(),
                'Order Type': order.getType(),
                'Action': "BUY" if order.isBuy() else "SELL",
                'Quantity': order.getQuantity(),
                'Market Price': marketPrice,
                'Limit Price': limitPrice,
                'Trigger Price': triggerPrice,
                'Fill Price': fillPrice,
                'Slippage': slippage,
                'Date/Time': dateTime
            })
            del self.orders[order]

    def _writeToFile(self, record: Dict):
        with open(self.filename, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(record)
