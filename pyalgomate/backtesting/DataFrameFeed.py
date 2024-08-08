"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import pandas as pd
from typing import List, Union, Tuple, Optional
import time
from pyalgotrade import bar
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.core import OptionType


class DataFrameFeed(BaseBarFeed):
    def __init__(
        self,
        completeDf: pd.DataFrame,
        df: pd.DataFrame,
        underlyings: List[str],
        frequency=bar.Frequency.MINUTE,
        maxLen=None,
        feedDelay: Union[float, None] = None,
        loadAll: bool = False,
    ):

        if frequency not in [bar.Frequency.MINUTE, bar.Frequency.DAY]:
            raise Exception("Invalid frequency")

        super(DataFrameFeed, self).__init__(frequency, maxLen)

        self.__completeDf: pd.DataFrame = completeDf.sort_values(
            ["Ticker", "Date/Time"]
        ).drop_duplicates(subset=["Ticker", "Date/Time"], keep="first")
        self.__df = df
        self.__frequency = frequency
        self.__haveAdjClose = False
        self.__instruments = self.__df["Ticker"].unique().tolist()
        self.__barsByDateTime = {}
        self.__feedDelay = feedDelay

        self.__currentDateTime = None
        self.__dateTimes = sorted(
            pd.to_datetime(self.__df["Date/Time"].unique()).tolist()
        )
        self.__nextPos = 0

        for instrument in underlyings:
            self.registerInstrument(instrument)

        self.__columnIndexMapping = {
            columnName: self.__df.columns.get_loc(columnName) + 1
            for columnName in self.__df.columns
        }

        for instrument in underlyings:
            self.addBars(instrument)

        if loadAll:
            for instrument in self.__instruments:
                self.addBars(instrument)

    def reset(self):
        self.__barsByDateTime = {}
        self.__currentDateTime = None
        self.__nextPos = 0
        super(DataFrameFeed, self).reset()

    def getApi(self):
        return None

    def barsHaveAdjClose(self):
        return self.__haveAdjClose

    def peekDateTime(self):
        return (
            self.__dateTimes[self.__nextPos]
            if self.__nextPos < len(self.__dateTimes)
            else None
        )

    def getCurrentDateTime(self):
        return (
            self.__currentDateTime
            if self.__currentDateTime is not None
            else self.peekDateTime()
        )

    def start(self):
        super(DataFrameFeed, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def eof(self):
        return self.__nextPos >= len(self.__dateTimes)

    def addBars(self, instrument) -> dict:
        if instrument not in self:
            self.registerInstrument(instrument)

        df = self.__df[(self.__df["Ticker"] == instrument)]

        for row in df.itertuples():
            instrument = row[self.__columnIndexMapping["Ticker"]]
            dateTime = row[self.__columnIndexMapping["Date/Time"]]
            open = row[self.__columnIndexMapping["Open"]]
            high = row[self.__columnIndexMapping["High"]]
            low = row[self.__columnIndexMapping["Low"]]
            close = row[self.__columnIndexMapping["Close"]]
            volume = row[self.__columnIndexMapping["Volume"]]
            openInterest = row[self.__columnIndexMapping["Open Interest"]]

            if dateTime not in self.__barsByDateTime:
                self.__barsByDateTime[dateTime] = dict()

            self.__barsByDateTime[dateTime][instrument] = bar.BasicBar(
                dateTime,
                open,
                high,
                low,
                close,
                volume,
                None,
                self.__frequency,
                extra={"Open Interest": openInterest},
            )

    def getNextBars(self):
        if self.__feedDelay:
            time.sleep(self.__feedDelay)

        currentDateTime = self.peekDateTime()

        if currentDateTime is None:
            return None

        self.__nextPos += 1
        self.__currentDateTime = pd.to_datetime(currentDateTime)

        if self.__currentDateTime not in self.__barsByDateTime:
            return None

        return bar.Bars(self.__barsByDateTime[self.__currentDateTime])

    def getLastBar(self, instrument) -> bar.Bar:
        lastBar = super().getLastBar(instrument)

        if lastBar is None:
            self.addBars(instrument)
            if (
                self.__currentDateTime not in self.__barsByDateTime
                or instrument not in self.__barsByDateTime[self.__currentDateTime]
            ):
                return None
            else:
                return self.__barsByDateTime[self.__currentDateTime][instrument]

        return lastBar

    def getLastUpdatedDateTime(self):
        return self.__currentDateTime

    def getLastReceivedDateTime(self):
        return self.__currentDateTime

    def getNextBarsDateTime(self):
        return self.__currentDateTime

    def isDataFeedAlive(self, heartBeatInterval=5):
        return True

    def getHistoricalData(
        self, instrument: str, timeDelta: datetime.timedelta, interval: str
    ) -> pd.DataFrame():
        if self.__completeDf is None:
            return pd.DataFrame(
                columns=[
                    "Date/Time",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Open Interest",
                ]
            )

        endDateTime = (
            self.__currentDateTime
            if self.__currentDateTime is not None
            else self.peekDateTime()
        )
        startDateTime = endDateTime - timeDelta

        mask = (
            (self.__completeDf["Date/Time"] > startDateTime)
            & (self.__completeDf["Date/Time"] < endDateTime)
            & (self.__completeDf["Ticker"] == instrument)
        )

        return (
            self.__completeDf[mask]
            .resample(f"{interval}min", on="Date/Time")
            .agg(
                {
                    "Open": "first",
                    "High": "max",
                    "Low": "min",
                    "Close": "last",
                    "Volume": "sum",
                    "Open Interest": "sum",
                }
            )
            .reset_index()
            .dropna()
        )

    def findNearestPremiumOption(
        self,
        expiry: datetime.datetime,
        optionType: OptionType,
        premium: float,
        time: datetime.datetime,
    ) -> Optional[Tuple[str, float]]:
        mask = (self.__completeDf["Date/Time"] == time) & (
            self.__completeDf["Ticker"].str.contains(
                expiry.strftime("%d%b%y").upper()
                + ("C" if optionType == OptionType.CALL else "P")
            )
        )
        filteredDf = self.__completeDf[mask].copy().reset_index()

        if filteredDf.empty:
            return None

        filteredDf.loc[:, "PremiumDiff"] = abs(filteredDf["Close"] - premium)

        minDiffIndex = filteredDf["PremiumDiff"].idxmin()
        nearestOption = filteredDf.loc[minDiffIndex]
        return str(nearestOption["Ticker"]), float(nearestOption["Close"])
