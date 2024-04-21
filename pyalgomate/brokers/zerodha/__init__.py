"""
.. moduleauthor:: Nagaraju Gunda
"""
import logging
import pyotp
from pyalgomate.brokers.zerodha.kiteext import KiteExt
from pyalgomate.brokers.zerodha.broker import getZerodhaTokensList, getOptionSymbols, getUnderlyingDetails
from pyalgomate.brokers.zerodha.feed import ZerodhaLiveFeed
from pyalgomate.brokers import getDefaultUnderlyings, getExpiryDates

logger = logging.getLogger()


def getFeed(cred, registerOptions, underlyings):
    api = KiteExt()
    twoFA = pyotp.TOTP(cred['factor2']).now()
    api.login_with_credentials(
        userid=cred['user'], password=cred['pwd'], twofa=twoFA)

    profile = api.profile()
    logger.info(f"Welcome {profile.get('user_name')}")

    if len(underlyings) == 0:
        underlyings = getDefaultUnderlyings()

    optionSymbols = []

    for underlying in underlyings:
        ltp = api.quote(underlying)[
            underlying]["last_price"]

        underlyingDetails = getUnderlyingDetails(underlying)
        index = underlyingDetails['index']
        strikeDifference = underlyingDetails['strikeDifference']
        (currentWeeklyExpiry, nextWeekExpiry, monthlyExpiry) = getExpiryDates(index)

        if "Weekly" in registerOptions:
            optionSymbols += getOptionSymbols(
                underlying, currentWeeklyExpiry, ltp, 10, strikeDifference)
        if "NextWeekly" in registerOptions:
            optionSymbols += getOptionSymbols(
                underlying, nextWeekExpiry, ltp, 10, strikeDifference)
        if "Monthly" in registerOptions:
            optionSymbols += getOptionSymbols(
                underlying, monthlyExpiry, ltp, 10, strikeDifference)

    optionSymbols = list(dict.fromkeys(optionSymbols))

    tokenMappings = getZerodhaTokensList(api, underlyings + optionSymbols)

    return ZerodhaLiveFeed(api, tokenMappings), api
