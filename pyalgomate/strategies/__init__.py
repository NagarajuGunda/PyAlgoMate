"""
.. moduleauthor:: Nagaraju Gunda
"""
import yaml
import datetime
from typing import List, Optional


class Target:
    def __init__(self, type_: str, targetProfit: Optional[int] = None):
        self.type = type_
        self.targetProfit = targetProfit

    def __repr__(self):
        return f'Target(type_={self.type}, targetProfit={self.targetProfit})'


class StopLoss:
    def __init__(self, type_: str, stopLoss: Optional[int] = None):
        self.type = type_
        self.stopLoss = stopLoss

    def __repr__(self):
        return f'StopLoss(type_={self.type}, stopLoss={self.stopLoss})'


class ReEntry:
    def __init__(self, type: str, count: int):
        self.type = type
        self.count = count

    def __repr__(self):
        return f'ReEntry(type={self.type}, count={self.count})'


class Position:
    def __init__(self, lots: int, buyOrSell: str, callOrPut: str, expiry: str, strikeType: str, strike: str, targetProfit: Target, stopLoss: StopLoss, reEntryOnTarget: ReEntry, reEntryOnSL: ReEntry):
        self.lots = lots
        self.buyOrSell = buyOrSell
        self.callOrPut = callOrPut
        self.expiry = expiry
        self.strikeType = strikeType
        self.strike = strike
        self.targetProfit = targetProfit
        self.stopLoss = stopLoss
        self.reEntryOnTarget = reEntryOnTarget
        self.reEntryOnSL = reEntryOnSL

    def __repr__(self):
        return f'Position(lots={self.lots}, buyOrSell={self.buyOrSell}, callOrPut={self.callOrPut}, expiry={self.expiry}, strikeType={self.strikeType}, strike={self.strike}, targetProfit={self.targetProfit}, stopLoss={self.stopLoss})'


class OptionStrategy:
    def __init__(self, instrument: str, strategyType: str, entryTime: datetime.datetime.time, exitTime: datetime.datetime.time, legwiseSL: dict, positions: List[Position], overallStopLoss: StopLoss, overallTrailSL: StopLoss, overallTarget: Target):
        self.instrument = instrument
        self.strategyType = strategyType
        self.entryTime = entryTime
        self.exitTime = exitTime
        self.legwiseSL = legwiseSL
        self.positions = positions
        self.overallStopLoss = overallStopLoss
        self.overallTrailSL = overallTrailSL
        self.overallTarget = overallTarget

    def __repr__(self):
        return f'OptionStrategy(instrument={self.instrument}, strategyType={self.strategyType}, entryTime={self.entryTime}, exitTime={self.exitTime}, legwiseSL={self.legwiseSL}, positions={self.positions}, overallStopLoss={self.overallStopLoss}, overallTrailSL={self.overallTrailSL}, overallTarget={self.overallTarget})'

    @classmethod
    def from_yaml_file(cls, file_path: str) -> 'OptionStrategy':
        with open(file_path, 'r') as f:
            content = yaml.safe_load(f)

        return cls(
            instrument=content['Instrument'],
            strategyType=content['StrategyType'],
            entryTime=datetime.datetime.strptime(
                content['EntryTime'], '%I:%M:%S %p').time(),
            exitTime=datetime.datetime.strptime(
                content['ExitTime'], '%I:%M:%S %p').time(),
            legwiseSL={
                'SquareOff': content['LegwiseSL']['SquareOff'],
                'TrailSLToBreakEven': content['LegwiseSL']['TrailSLToBreakEven']
            },
            positions=[
                Position(
                    lots=p['Lots'],
                    buyOrSell=p['BuyOrSell'],
                    callOrPut=p['CallOrPut'],
                    expiry=p['Expiry'],
                    strikeType=p['StrikeType'],
                    strike=p['Strike'],
                    targetProfit=Target(
                        p['TargetProfit']['Type'], p['TargetProfit']['TargetProfit']),
                    stopLoss=StopLoss(p['StopLoss']['Type'],
                                      p['StopLoss']['StopLoss']),
                    reEntryOnTarget=(ReEntry(p['ReEntryOnTarget']['Type'], p['ReEntryOnTarget']['Count']) if p.get(
                        'ReEntryOnTarget', None) is not None else None),
                    reEntryOnSL=(ReEntry(p['ReEntryOnSL']['Type'], p['ReEntryOnSL']['Count']) if p.get(
                        'ReEntryOnSL', None) is not None else None)
                ) for p in content['Positions']
            ],
            overallStopLoss=StopLoss(
                content['OverallStopLoss']['Type'], content['OverallStopLoss']['OverallStopLoss']),
            overallTrailSL=content['OverallTrailSL'],
            overallTarget=Target(
                content['OverallTarget']['Type'], content['OverallTarget']['OverallTarget'])
        )


class OptionContract:
    def __init__(self, symbol: str, strike: int, expiry: datetime.date, type: str, underlying: str):
        self.symbol = symbol
        self.strike = strike
        self.expiry = expiry
        self.type = type
        self.underlying = underlying

    def __repr__(self):
        return f'OptionContract(symbol={self.symbol}, strike={self.strike}, expiry={self.expiry}, type = {self.type}, underlying={self.underlying})'


class OptionGreeks:
    def __init__(self, optionContract: OptionContract, price: float, delta: float, gamma: float, theta: float, vega: float, iv: float, oi: float = 0):
        self.optionContract = optionContract
        self.price = price
        self.delta = delta
        self.gamma = gamma
        self.theta = theta
        self.vega = vega
        self.iv = iv
        self.oi = oi

    def __repr__(self):
        return f'OptionGreeks(optionContract={self.optionContract}, price={self.price}, delta={self.delta}, gamma={self.gamma}, theta={self.theta}, vega={self.vega}, iv={self.iv}, oi={self.oi}'
