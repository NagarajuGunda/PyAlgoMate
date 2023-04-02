from pyalgotrade import stratanalyzer
from pyalgotrade import broker

import numpy as np

from pprint import pprint

class DayWise(stratanalyzer.StrategyAnalyzer):
    def __init__(self):
        super(DayWise, self).__init__()

    def __onOrderEvent(self, broker_, orderEvent):
        # Only interested in filled or partially filled orders.
        if orderEvent.getEventType() not in (broker.OrderEvent.Type.PARTIALLY_FILLED, broker.OrderEvent.Type.FILLED):
            return
        
        pprint(vars(orderEvent))
        pprint(vars(orderEvent.getOrder()))

    def attached(self, strat):
        strat.getBroker().getOrderUpdatedEvent().subscribe(self.__onOrderEvent)
