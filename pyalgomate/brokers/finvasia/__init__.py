"""
.. moduleauthor:: Nagaraju Gunda
"""

import datetime
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from io import BytesIO, StringIO
from zipfile import ZipFile

import pandas as pd
import pyotp
import requests
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

import pyalgomate.utils as utils
from pyalgomate.brokers import getDefaultUnderlyings, getExpiryDates
from pyalgomate.utils import UnderlyingIndex

logger = logging.getLogger()

urls = [
    "https://api.shoonya.com/NSE_symbols.txt.zip",
    "https://api.shoonya.com/NFO_symbols.txt.zip",
    "https://api.shoonya.com/CDS_symbols.txt.zip",
    "https://api.shoonya.com/MCX_symbols.txt.zip",
    "https://api.shoonya.com/BSE_symbols.txt.zip",
    "https://api.shoonya.com/BFO_symbols.txt.zip",
]

underlyingMapping = {
    'NSE|MIDCPNIFTY': {
        'optionPrefix': 'NFO|MIDCPNIFTY',
        'index': UnderlyingIndex.MIDCPNIFTY,
        'lotSize': 50,
        'strikeDifference': 25
    },
    'NSE|NIFTY BANK': {
        'optionPrefix': 'NFO|BANKNIFTY',
        'index': UnderlyingIndex.BANKNIFTY,
        'lotSize': 15,
        'strikeDifference': 100
    },
    'NSE|NIFTY INDEX': {
        'optionPrefix': 'NFO|NIFTY',
        'index': UnderlyingIndex.NIFTY,
        'lotSize': 25,
        'strikeDifference': 50
    },
    'NSE|FINNIFTY': {
        'optionPrefix': 'NFO|FINNIFTY',
        'index': UnderlyingIndex.FINNIFTY,
        'lotSize': 25,
        'strikeDifference': 50
    },
    'BSE|SENSEX': {
        'optionPrefix': 'BFO|SENSEX',
        'index': UnderlyingIndex.SENSEX,
        'lotSize': 10,
        'strikeDifference': 100
    },
    'BSE|BANKEX': {
        'optionPrefix': 'BFO|BANKEX',
        'index': UnderlyingIndex.BANKEX,
        'lotSize': 15,
        'strikeDifference': 100
    }
}

def downloadAndExtract(url):
    logger.info(f'Downloading and extracting {url}')
    response = requests.get(url)
    with ZipFile(BytesIO(response.content)) as z:
        fileName = z.namelist()[0]
        with z.open(fileName) as f:
            content = f.read().decode("utf-8")
            df = pd.read_csv(StringIO(content), delimiter=",")
    return df

@lru_cache(maxsize=None)
def getScriptMaster(scripMasterFile='finvasia_symbols.csv') -> pd.DataFrame:
    if os.path.exists(scripMasterFile) and \
            (datetime.datetime.fromtimestamp(os.path.getmtime(scripMasterFile)).date() == datetime.datetime.today().date()):
        logger.info(
            "Scrip master that has been created today already exists. Using the same!"
        )
        return pd.read_csv(scripMasterFile)
    else:
        logger.info(
            "Scrip master either doesn't exist or is not of today. Downloading!"
        )
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(downloadAndExtract, url)
                       for url in urls]

            dfs = []
            for future in futures:
                df = future.result()
                dfs.append(df)

            scripMasterDf = pd.concat(dfs, ignore_index=True)
            scripMasterDf.to_csv(scripMasterFile, index=False)
            return scripMasterDf

def getToken(instrument) -> str:
    tokenMappings = getTokenMappings()
    return tokenMappings[instrument]

def getTokenMappings() -> dict:
    scripMasterDf: pd.DataFrame = getScriptMaster()
    tokenMappings = dict(zip(scripMasterDf['Exchange'] + '|' + scripMasterDf['TradingSymbol'],
                         scripMasterDf['Exchange'] + '|' + scripMasterDf['Token'].astype(str)))
    tokenMappings['BSE|SENSEX'] = 'BSE|1'
    tokenMappings['BSE|BANKEX'] = 'BSE|12'
    return tokenMappings

def getApi(cred):
    api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                     websocket='wss://api.shoonya.com/NorenWSTP/')
    userToken = None
    tokenFile = 'shoonyakey.txt'
    if os.path.exists(tokenFile) and (datetime.datetime.fromtimestamp(os.path.getmtime(tokenFile)).date() == datetime.datetime.today().date()):
        logger.info("Token has been created today already. Re-using it")
        with open(tokenFile, 'r') as f:
            userToken = f.read()
        logger.info(
            f"userid {cred['user']} password ******** usertoken {userToken}")
        loginStatus = api.set_session(
            userid=cred['user'], password=cred['pwd'], usertoken=userToken)
    else:
        logger.info("Logging in and persisting user token")
        loginStatus = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                                vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

        if loginStatus:
            with open(tokenFile, 'w') as f:
                f.write(loginStatus.get('susertoken'))

            logger.info(
                f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")
        else:
            logger.info("Login failed!")

    if loginStatus != None:
        return api
    else:
        return None

def getApiAndTokenMappings(cred, registerOptions, underlyings):
    from .broker import getOptionSymbols, getUnderlyingDetails  # Lazy import

    api = getApi(cred)
    if api != None:
        if len(underlyings) == 0:
            underlyings = [underlying.replace(
                ":", "|") for underlying in getDefaultUnderlyings()]

        optionSymbols = []
        futureSymbols = []
        tokenMappings = getTokenMappings()
        for underlying in underlyings:
            exchange = underlying.split('|')[0]
            underlyingToken = tokenMappings[underlying].split('|')[1]
            logger.info(
                f'Token id for <{underlying}> is <{underlyingToken}>')
            if underlyingToken is None:
                logger.error(
                    f'Error getting token id for {underlyingToken}')
                exit(1)
            underlyingQuotes = api.get_quotes(exchange, underlyingToken)
            ltp = underlyingQuotes['lp']

            try:
                underlyingDetails = getUnderlyingDetails(
                    underlying)
                index = underlyingDetails['index']
                strikeDifference = underlyingDetails['strikeDifference']

                (currentWeeklyExpiry, nextWeekExpiry,
                 monthlyExpiry) = getExpiryDates(index)
                futureSymbol = getFutureSymbol(index, monthlyExpiry)
                futureSymbols.append(futureSymbol)
                if "Weekly" in registerOptions:
                    optionSymbols += getOptionSymbols(
                        underlying, currentWeeklyExpiry, ltp, 20, strikeDifference)
                if "NextWeekly" in registerOptions:
                    optionSymbols += getOptionSymbols(
                        underlying, nextWeekExpiry, ltp, 20, strikeDifference)
                if "Monthly" in registerOptions:
                    optionSymbols += getOptionSymbols(
                        underlying, monthlyExpiry, ltp, 20, strikeDifference)
            except Exception as e:
                logger.exception(f'Exception: {e}')

        optionSymbols = list(dict.fromkeys(optionSymbols))

        instruments = underlyings + futureSymbols + optionSymbols

        filteredTokenMappings = {
            tokenMappings[instrument]: instrument for instrument in instruments if instrument in tokenMappings}

        return api, filteredTokenMappings
    else:
        exit(1)

def getFeed(cred, registerOptions, underlyings):
    from .feed import LiveTradeFeed

    logger.info('Creating feed object')
    api, tokenMappings = getApiAndTokenMappings(
        cred, registerOptions, underlyings)
    return LiveTradeFeed(api, getTokenMappings(), tokenMappings.values()), api

@lru_cache
def getOptionContract(symbol):
    from .broker import OptionContract

    m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", symbol)

    if m is None:
        m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d+)([CP])E", symbol)

        if m is not None:
            optionPrefix = m.group(1)
            for underlying, underlyingDetails in underlyingMapping.items():
                if underlyingDetails['optionPrefix'] == optionPrefix:
                    index = underlyingDetails['index']
                    month = datetime.datetime.strptime(m.group(3), '%b').month
                    year = int(m.group(2)) + 2000
                    expiry = utils.getNearestMonthlyExpiryDate(
                        datetime.date(year, month, 1), index)
                    return OptionContract(symbol, int(m.group(4)), expiry, "c" if m.group(5) == "C" else "p", underlying)

        m = re.match(r"([A-Z\|]+)(\d{2})(\d|[OND])(\d{2})(\d+)([CP])E", symbol)

        if m is None:
            return None

        day = int(m.group(4))
        month = m.group(3)
        if month == 'O':
            month = 10
        elif month == 'N':
            month = 11
        elif month == 'D':
            month = 12
        else:
            month = int(month)

        year = int(m.group(2)) + 2000
        expiry = datetime.date(year, month, day)
        optionPrefix = m.group(1)
        for underlying, underlyingDetails in underlyingMapping.items():
            if underlyingDetails['optionPrefix'] == optionPrefix:
                return OptionContract(symbol, int(m.group(5)), expiry, "c" if m.group(6) == "C" else "p", underlying)

    day = int(m.group(2))
    month = m.group(3)
    year = int(m.group(4)) + 2000
    expiry = datetime.date(
        year, datetime.datetime.strptime(month, '%b').month, day)

    optionPrefix = m.group(1)

    for underlying, underlyingDetails in underlyingMapping.items():
        if underlyingDetails['optionPrefix'] == optionPrefix:
            return OptionContract(symbol, int(m.group(6)), expiry, "c" if m.group(5) == "C" else "p", underlying)


def getFutureSymbol(underlyingIndex: UnderlyingIndex, expiry: datetime.date):
    scripMasterDf: pd.DataFrame = getScriptMaster()
    scripMasterDf["Expiry"] = pd.to_datetime(scripMasterDf["Expiry"], format="%d-%b-%Y")
    futureRow = scripMasterDf[
        (scripMasterDf["Expiry"].dt.date == expiry)
        & (scripMasterDf["Instrument"] == "FUTIDX")
        & scripMasterDf["TradingSymbol"].str.startswith(str(underlyingIndex))
    ].iloc[0]
    return futureRow["Exchange"] + "|" + futureRow["TradingSymbol"]


if __name__ == '__main__':
    print(getScriptMaster())
