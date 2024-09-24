from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Type, Union

from talipp.ohlcv import OHLCV


@dataclass
class IndicatorConfig:
    use_ohlcv: bool


class IndicatorManager:
    def __init__(self):
        self.indicators: Dict[int, Dict[str, Dict[Type, Any]]] = {}
        self.indicator_configs: Dict[Type, IndicatorConfig] = {}
        self.candles: Dict[int, Dict[str, List[OHLCV]]] = {}

    def register_indicator(self, indicator_class: Type, use_ohlcv: bool):
        """Register a new indicator type."""
        self.indicator_configs[indicator_class] = IndicatorConfig(use_ohlcv)

    def add_timeframe(self, timeframe: int):
        if timeframe not in self.indicators:
            self.indicators[timeframe] = {}
        if timeframe not in self.candles:
            self.candles[timeframe] = {}

    def add_instrument(self, timeframe: int, instrument: str):
        if timeframe not in self.indicators:
            raise ValueError(
                f"Timeframe {timeframe} not found. Add it first using add_timeframe()."
            )
        if instrument not in self.indicators[timeframe]:
            self.indicators[timeframe][instrument] = {}
        if instrument not in self.candles[timeframe]:
            self.candles[timeframe][instrument] = []

    def add_indicator(
        self,
        timeframe: int,
        instrument: str,
        indicator_class: Type,
        params: Dict[str, any],
    ):
        if timeframe not in self.indicators:
            raise ValueError(
                f"Timeframe {timeframe} not found. Add it first using add_timeframe()."
            )
        if instrument not in self.indicators[timeframe]:
            raise ValueError(
                f"Instrument {instrument} not found for timeframe {timeframe}. Add it first using add_instrument()."
            )
        if indicator_class in self.indicators[timeframe][instrument]:
            return

        indicator_config = self.indicator_configs.get(indicator_class)
        if not indicator_config:
            raise ValueError(
                f"Unsupported indicator type: {indicator_class.__name__}. Register it first using register_indicator()."
            )

        indicator = indicator_class(**params)
        self.indicators[timeframe][instrument][indicator_class] = indicator

    def update_data(
        self,
        timeframe: int,
        instrument: str,
        dateTime: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ):
        if (
            timeframe not in self.indicators
            or instrument not in self.indicators[timeframe]
        ):
            raise ValueError(
                f"Timeframe {timeframe} or instrument {instrument} not found. Add them first using add_timeframe() and add_instrument()."
            )

        ohlcv = OHLCV(
            time=dateTime, open=open, high=high, low=low, close=close, volume=volume
        )
        self.candles[timeframe][instrument].append(ohlcv)
        self._update_indicators(timeframe, instrument, ohlcv)

    def is_instrument_registered(self, timeframe: int, instrument: str):
        return timeframe in self.indicators and instrument in self.indicators[timeframe]

    def get_indicator_value(
        self, timeframe: int, instrument: str, indicator_class: Type, index: int = -1
    ) -> Union[float, List[float], None]:
        if (
            timeframe not in self.indicators
            or instrument not in self.indicators[timeframe]
            or indicator_class not in self.indicators[timeframe][instrument]
        ):
            raise ValueError(
                f"Indicator {indicator_class.__name__} not found for instrument {instrument} in timeframe {timeframe}."
            )

        indicator = self.indicators[timeframe][instrument][indicator_class]
        return indicator[index] if len(indicator) > 0 else None

    def get_indicator_values(
        self, timeframe: int, instrument: str, indicator_class: Type
    ) -> Union[List[Any], None]:
        if (
            timeframe not in self.indicators
            or instrument not in self.indicators[timeframe]
            or indicator_class not in self.indicators[timeframe][instrument]
        ):
            raise ValueError(
                f"Indicator {indicator_class.__name__} not found for instrument {instrument} in timeframe {timeframe}."
            )

        indicator = self.indicators[timeframe][instrument][indicator_class]
        if hasattr(indicator, "output_values"):
            return indicator.output_values
        elif isinstance(indicator, list):
            return indicator
        else:
            return list(indicator)

    def get_indicator(
        self, timeframe: int, instrument: str, indicator_class: Type
    ) -> Union[Any, None]:
        if (
            timeframe not in self.indicators
            or instrument not in self.indicators[timeframe]
            or indicator_class not in self.indicators[timeframe][instrument]
        ):
            raise ValueError(
                f"Indicator {indicator_class.__name__} not found for instrument {instrument} in timeframe {timeframe}."
            )

        return self.indicators[timeframe][instrument][indicator_class]

    def get_candles(
        self,
        timeframe: int,
        instrument: str,
        start_index: int = 0,
        end_index: int = None,
    ) -> List[OHLCV]:
        if timeframe not in self.candles or instrument not in self.candles[timeframe]:
            raise ValueError(
                f"Candles not found for instrument {instrument} in timeframe {timeframe}."
            )

        candles = self.candles[timeframe][instrument]
        if end_index is None:
            return candles[start_index:]
        return candles[start_index:end_index]

    def _update_indicators(self, timeframe: int, instrument: str, ohlcv: OHLCV):
        for indicator_class, indicator in self.indicators[timeframe][
            instrument
        ].items():
            self._calculate_indicator(indicator_class, indicator, ohlcv)

    def _calculate_indicator(self, indicator_class: Type, indicator: Any, ohlcv: OHLCV):
        indicator_config = self.indicator_configs[indicator_class]

        if indicator_config.use_ohlcv:
            indicator.add(ohlcv)
        else:
            indicator.add(ohlcv.close)
