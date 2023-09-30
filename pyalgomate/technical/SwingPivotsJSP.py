import datetime
from dataclasses import dataclass


@dataclass
class Bar:
    datetime: datetime.datetime
    open: float
    high: float
    low: float
    close: float
    fdatetime: datetime.datetime


class SwingPivotsJSP:
    def __init__(self):
        self.data = list()
        self.pivotLows = list()
        self.pivotHighs = list()
        self.largePivotLows = list()
        self.largePivotHighs = list()

        self.__reset__()

    def __reset__(self):
        self.pivotLowAnchor = None
        self.pivotHighAnchor = None
        self.high = self.low = None

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
        self.data.append(Bar(dateTime, open, high, low, close, None))
        self.calculatePivots()

    def findPivotLow(self, anchorBar, previousBars, nextBars) -> bool:
        if self.pivotLowAnchor and nextBars[1].low > self.pivotLowAnchor.low and anchorBar.low > self.pivotLowAnchor.low:
            if nextBars[1].high > self.high:
                if self.pivotHighs[-1].datetime > self.pivotLows[-1].datetime:
                    self.pivotLows.append(self.pivotLowAnchor)
                elif self.pivotLowAnchor.low < self.pivotLows[-1].low:
                    self.pivotLows[-1] = self.pivotLowAnchor
                self.pivotLows[-1].fdatetime = self.data[-1].datetime
                self.__reset__()
                return True
        else:
            isLocalBottom = (
                anchorBar.low < min(
                    [bar.low for bar in previousBars + nextBars])
            )

            if isLocalBottom:
                if anchorBar.close > anchorBar.open:
                    if nextBars[0].high > anchorBar.high or nextBars[1].high > anchorBar.high:
                        if self.pivotHighs[-1].datetime > self.pivotLows[-1].datetime:
                            self.pivotLows.append(anchorBar)
                        elif anchorBar.low < self.pivotLows[-1].low:
                            self.pivotLows[-1] = anchorBar
                        self.pivotLows[-1].fdatetime = self.data[-1].datetime
                        self.__reset__()
                        return True
                    else:
                        self.pivotLowAnchor = anchorBar
                        self.high = anchorBar.high
                elif nextBars[0].close > nextBars[0].open:
                    if nextBars[1].high > nextBars[0].high:
                        if self.pivotHighs[-1].datetime > self.pivotLows[-1].datetime:
                            self.pivotLows.append(anchorBar)
                        elif anchorBar.low < self.pivotLows[-1].low:
                            self.pivotLows[-1] = anchorBar
                        self.pivotLows[-1].fdatetime = self.data[-1].datetime
                        self.__reset__()
                        return True
                    else:
                        self.pivotLowAnchor = anchorBar
                        self.high = nextBars[0].high
                elif nextBars[1].close > nextBars[1].open:
                    self.pivotLowAnchor = anchorBar
                    self.high = nextBars[1].high

        return False

    def findPivotHigh(self, anchorBar, previousBars, nextBars) -> bool:
        if self.pivotHighAnchor and nextBars[1].high < self.pivotHighAnchor.high and anchorBar.high < self.pivotHighAnchor.high:
            if nextBars[1].low < self.low:
                if self.pivotLows[-1].datetime > self.pivotHighs[-1].datetime:
                    self.pivotHighs.append(self.pivotHighAnchor)
                elif self.pivotHighAnchor.high > self.pivotHighs[-1].high:
                    self.pivotHighs[-1] = self.pivotHighAnchor
                self.pivotHighs[-1].fdatetime = self.data[-1].datetime
                self.__reset__()
                return True
        else:
            isLocalTop = (
                anchorBar.high > max(
                    [bar.high for bar in previousBars + nextBars])
            )

            if isLocalTop:
                if anchorBar.close < anchorBar.open:
                    if nextBars[0].low < anchorBar.low or nextBars[1].low < anchorBar.low:
                        if self.pivotLows[-1].datetime > self.pivotHighs[-1].datetime:
                            self.pivotHighs.append(anchorBar)
                        elif anchorBar.high > self.pivotHighs[-1].high:
                            self.pivotHighs[-1] = anchorBar
                        self.pivotHighs[-1].fdatetime = self.data[-1].datetime
                        self.__reset__()
                        return True
                    else:
                        self.pivotHighAnchor = anchorBar
                        self.low = anchorBar.low
                elif nextBars[0].close < nextBars[0].open:
                    if nextBars[1].low < nextBars[0].low:
                        if self.pivotLows[-1].datetime > self.pivotHighs[-1].datetime:
                            self.pivotHighs.append(anchorBar)
                        elif anchorBar.high > self.pivotHighs[-1].high:
                            self.pivotHighs[-1] = anchorBar
                        self.pivotHighs[-1].fdatetime = self.data[-1].datetime
                        self.__reset__()
                        return True
                    else:
                        self.pivotHighAnchor = anchorBar
                        self.low = nextBars[0].low
                elif nextBars[1].close < nextBars[1].open:
                    self.pivotHighAnchor = anchorBar
                    self.low = nextBars[1].low

        return False

    def calculatePivots(self):
        if len(self.data) < 5:
            return

        if not len(self.pivotLows) and not len(self.pivotHighs):
            if self.data[0].high > self.data[1].high:
                self.pivotHighs.append(self.data[0])
                self.pivotLows.append(self.data[1])
                self.largePivotHighs.append(self.data[0])
                self.largePivotLows.append(self.data[1])
                self.pivotHighs[-1].fdatetime = self.data[-1].datetime
                self.pivotLows[-1].fdatetime = self.data[-1].datetime
                self.largePivotHighs[-1].fdatetime = self.data[-1].datetime
                self.largePivotLows[-1].fdatetime = self.data[-1].datetime
            else:
                self.pivotHighs.append(self.data[1])
                self.pivotLows.append(self.data[0])
                self.largePivotHighs.append(self.data[1])
                self.largePivotLows.append(self.data[0])
                self.pivotHighs[-1].fdatetime = self.data[-1].datetime
                self.pivotLows[-1].fdatetime = self.data[-1].datetime
                self.largePivotHighs[-1].fdatetime = self.data[-1].datetime
                self.largePivotLows[-1].fdatetime = self.data[-1].datetime
            return

        # The anchor bar (most recent)
        anchorBar = self.data[-2-1]

        # Bars before the current bar
        previousBars = list(
            self.data)[-2*2-1:-2-1]

        # Bars after the current bar
        nextBars = list(self.data)[-2:]

        if not self.findPivotLow(anchorBar, previousBars, nextBars):
            self.findPivotHigh(anchorBar, previousBars, nextBars)

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
                self.largePivotLows[-1].fdatetime = self.data[-1].datetime
                return

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
                self.largePivotHighs[-1].fdatetime = self.data[-1].datetime
                return


if __name__ == "__main__":
    import pandas as pd

    data = pd.read_parquet('strategies/data/2023/banknifty/08.parquet')
    data = data[data['Ticker'] == 'BANKNIFTY']
    data = (
        data.resample("15min", on="Date/Time")
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
            .reset_index()
            .dropna()
    )

    SwingPivotsJSP = SwingPivotsJSP()
    for index, row in data.iterrows():
        SwingPivotsJSP.add_input_value(
            row['Date/Time'], row['Open'], row['High'], row['Low'], row['Close'])

    merged = SwingPivotsJSP.getPivotHighs() + SwingPivotsJSP.getPivotLows()
    merged.sort(key=lambda x: x.datetime)

    [print(f"{'PH' if item in SwingPivotsJSP.getPivotHighs() else 'PL'} - {item}")
     for item in merged]

    merged = SwingPivotsJSP.getLargePivotHighs() + SwingPivotsJSP.getLargePivotLows()
    merged.sort(key=lambda x: x.datetime)

    [print(f"{'LPH' if item in SwingPivotsJSP.getPivotHighs() else 'LPL'} - {item}")
     for item in merged]

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close, bar.fdatetime)
    #      for bar in SwingPivotsJSP.getPivotHighs()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'FDateTime']).to_csv('pivot_highs.csv', index=False)

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close, bar.fdatetime)
    #      for bar in SwingPivotsJSP.getPivotLows()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'FDateTime']).to_csv('pivot_lows.csv', index=False)

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close, bar.fdatetime)
    #      for bar in SwingPivotsJSP.getLargePivotHighs()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'FDateTime']).to_csv('large_pivot_highs.csv', index=False)

    # pd.DataFrame(
    #     [(bar.datetime, bar.open, bar.high, bar.low, bar.close, bar.fdatetime)
    #      for bar in SwingPivotsJSP.getLargePivotLows()],
    #     columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'FDateTime']).to_csv('large_pivot_lows.csv', index=False)
