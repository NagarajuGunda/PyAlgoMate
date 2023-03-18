import logging
import numpy as np

from pyalgotrade import strategy
import pyalgomate.utils as utils
from pyalgomate.strategies import OptionGreeks
from py_vollib_vectorized import vectorized_implied_volatility, get_all_greeks


class BaseOptionsGreeksStrategy(strategy.BaseStrategy):

    def __init__(self, feed, broker):
        super(BaseOptionsGreeksStrategy, self).__init__(feed, broker)

        self.__optionData = dict()

    def calculateGreeks(self, bars):
        # Collect all the necessary data into NumPy arrays
        optionContracts = []
        underlyingPrices = []
        strikes = []
        prices = []
        expiries = []
        types = []
        for instrument, bar in bars.items():
            optionContract = self.getBroker().getOptionContract(instrument)
            if optionContract is not None:
                optionContracts.append(optionContract)
                strikes.append(optionContract.strike)
                prices.append(bar.getClose())
                underlyingPrices.append(self.getFeed().getDataSeries(
                    optionContract.underlying)[-1].getClose())
                if optionContract.expiry is None:
                    expiry = utils.getNearestMonthlyExpiryDate(
                        bar.getDateTime().date())
                else:
                    expiry = optionContract.expiry
                expiries.append(
                    (expiry - bar.getDateTime().date()).days / 365.0)
                types.append(optionContract.type)
        underlyingPrices = np.array(underlyingPrices)
        strikes = np.array(strikes)
        prices = np.array(prices)
        expiries = np.array(expiries)
        types = np.array(types)

        try:
            # Calculate implied volatilities
            iv = vectorized_implied_volatility(prices, underlyingPrices, strikes, expiries, 0.0,
                                            types, q=0, model='black_scholes_merton', return_as='numpy', on_error='ignore')

            # Calculate greeks
            greeks = get_all_greeks(types, underlyingPrices, strikes, expiries,
                                    0.0, iv, 0.0, model='black_scholes', return_as='dict')
        except:
            return

        # Store the results
        for i in range(len(optionContracts)):
            optionContract = optionContracts[i]
            symbol = optionContract.symbol
            deltaVal = greeks['delta'][i]
            gammaVal = greeks['gamma'][i]
            thetaVal = greeks['theta'][i]
            vegaVal = greeks['vega'][i]
            ivVal = iv[i]
            self.__optionData[symbol] = OptionGreeks(
                optionContract, prices[i], deltaVal, gammaVal, thetaVal, vegaVal, ivVal)

    def getOptionData(self, bars):
        self.calculateGreeks(bars)
        return self.__optionData
