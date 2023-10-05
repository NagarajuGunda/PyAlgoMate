import datetime
from dataclasses import dataclass


@dataclass
class Bar:
    datetime: datetime.datetime
    open: float
    high: float
    low: float
    close: float


class StructuralPivots:
    def __init__(self, lookupPeriod: int = 2):
        self.lookupPeriod = lookupPeriod
        self.data = list()
        self.pivotLows = list()
        self.pivotHighs = list()
        self.largePivotLows = list()
        self.largePivotHighs = list()

    def getPivotHighs(self):
        return self.pivotHighs

    def getPivotLows(self):
        return self.pivotLows

    def getLargePivotHighs(self):
        return self.largePivotHighs

    def getLargePivotLows(self):
        return self.largePivotLows

    def getBars(self):
        return self.data

    def add_input_value(self, dateTime: datetime.datetime, open: float, high: float, low: float, close: float):
        self.data.append(Bar(dateTime, open, high, low, close))
        self.calculatePivots()

    def calculatePivots(self):
        if not len(self.pivotHighs) and len(self.data):
            self.pivotHighs.append(self.data[0])

        if not len(self.pivotLows) and len(self.data):
            self.pivotLows.append(self.data[0])

        if len(self.data) < self.lookupPeriod * 2 + 1:
            return

       # The middle bar (most recent)
        middleBar = self.data[-self.lookupPeriod-1]

        # Bars before the current bar
        previousBars = list(
            self.data)[-self.lookupPeriod*2-1:-self.lookupPeriod-1]

        # Bars after the current bar
        nextBars = list(self.data)[-self.lookupPeriod:]

        isLocalBottom = (
            middleBar.low < min([bar.low for bar in previousBars + nextBars])
        )

        if isLocalBottom:
            self.pivotLows.append(middleBar)

        isLocalTop = (
            middleBar.high > max([bar.high for bar in previousBars + nextBars])
        )

        if isLocalTop:
            self.pivotHighs.append(middleBar)

        if not len(self.largePivotLows) and len(self.pivotLows):
            self.largePivotLows.append(self.pivotLows[-1])

        if not len(self.largePivotHighs) and len(self.pivotHighs):
            self.largePivotHighs.append(self.pivotHighs[-1])

        if not len(self.largePivotHighs) or not len(self.largePivotLows):
            return

        if len(self.pivotHighs) > 0 and self.data[-1].high > self.pivotHighs[-1].high:
            largePivotHigh = self.largePivotHighs[-1]
            llvBar = None
            for bar in reversed(self.data[:-1]):
                if bar == largePivotHigh:
                    break
                if llvBar is None or bar.low < llvBar.low:
                    llvBar = bar

            if llvBar and self.largePivotLows[-1] != llvBar:
                self.largePivotLows.append(llvBar)

        if len(self.pivotLows) > 0 and self.data[-1].low < self.pivotLows[-1].low and len(self.pivotHighs):
            largePivotLow = self.largePivotLows[-1]
            hhvBar = None
            for bar in reversed(self.data[:-1]):
                if bar == largePivotLow:
                    break
                if hhvBar is None or bar.high > hhvBar.high:
                    hhvBar = bar

            if hhvBar and self.largePivotHighs[-1] != hhvBar:
                self.largePivotHighs.append(hhvBar)


if __name__ == "__main__":
    import pandas as pd

    data = pd.read_parquet('strategies/data/2023/nifty/06.parquet')
    data = data[data['Ticker'] == 'NIFTY']
    data = (
        data.resample("5min", on="Date/Time")
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
            .reset_index()
            .dropna()
    )

    structuralPivots = StructuralPivots()
    for index, row in data.iterrows():
        structuralPivots.add_input_value(
            row['Date/Time'], row['Open'], row['High'], row['Low'], row['Close'])

    [print(item) for item in structuralPivots.getPivotHighs()]
    [print(item) for item in structuralPivots.getPivotLows()]
    [print(item) for item in structuralPivots.getLargePivotHighs()]
    [print(item) for item in structuralPivots.getLargePivotLows()]

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close)
    #      for bar in structuralPivots.getPivotHighs()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close']).to_csv('pivot_highs.csv', index=False)

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close)
    #      for bar in structuralPivots.getPivotLows()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close']).to_csv('pivot_lows.csv', index=False)

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close)
    #      for bar in structuralPivots.getLargePivotHighs()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close']).to_csv('large_pivot_highs.csv', index=False)

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close)
    #      for bar in structuralPivots.getLargePivotLows()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close']).to_csv('large_pivot_lows.csv', index=False)
