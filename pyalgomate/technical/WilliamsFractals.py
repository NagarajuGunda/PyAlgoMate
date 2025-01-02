from typing import List
from talipp.ohlcv import OHLCV


class WilliamsFractals:
    def __init__(self, period=2):
        self.period = period
        self._highs = []
        self._lows = []
        self._candles: List[OHLCV] = []
        self.bullish_fractals = []
        self.bearish_fractals = []
        self.output_values = []

    def __getitem__(self, index):
        return self.output_values[index]

    def __len__(self):
        return len(self.output_values)

    def add(self, ohlcv: OHLCV):
        self._candles.append(ohlcv)

        if len(self._candles) < 2 * self.period + 1:
            return None

        # Check for bearish fractal (high point)
        middle_high = self._candles[-self.period - 1].high
        left_highs = [
            c.high for c in self._candles[-2 * self.period - 1 : -self.period - 1]
        ]
        right_highs = [c.high for c in self._candles[-self.period :]]

        if all(h < middle_high for h in left_highs) and all(
            h < middle_high for h in right_highs
        ):
            self.bearish_fractals.append(
                (self._candles[-self.period - 1].time, middle_high)
            )

        # Check for bullish fractal (low point)
        middle_low = self._candles[-self.period - 1].low
        left_lows = [
            c.low for c in self._candles[-2 * self.period - 1 : -self.period - 1]
        ]
        right_lows = [c.low for c in self._candles[-self.period :]]

        if all(low > middle_low for low in left_lows) and all(
            low > middle_low for low in right_lows
        ):
            self.bullish_fractals.append(
                (self._candles[-self.period - 1].time, middle_low)
            )
