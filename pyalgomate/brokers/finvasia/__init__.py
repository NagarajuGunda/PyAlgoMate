"""
.. moduleauthor:: Nagaraju Gunda
"""

import os
import datetime
import requests
import logging
from io import BytesIO, StringIO
from zipfile import ZipFile
import pandas as pd
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
import pyotp
from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
from pyalgomate.brokers import getDefaultUnderlyings, getExpiryDates
from pyalgomate.brokers.finvasia.broker import getOptionSymbols, getUnderlyingDetails
from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
import pyalgomate.utils as utils

logger = logging.getLogger()

urls = [
    "https://api.shoonya.com/NSE_symbols.txt.zip",
    "https://api.shoonya.com/NFO_symbols.txt.zip",
    "https://api.shoonya.com/CDS_symbols.txt.zip",
    "https://api.shoonya.com/MCX_symbols.txt.zip",
    "https://api.shoonya.com/BSE_symbols.txt.zip",
    "https://api.shoonya.com/BFO_symbols.txt.zip",
]

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
        logger.info(f'Scrip master that has been created today already exists. Using the same!')
        return pd.read_csv(scripMasterFile)
    else:
        logger.info(f'Scrip master either doesn\'st exit of is not of today. Downloading!')
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(downloadAndExtract, url) for url in urls]

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
    tokenMappings = dict(zip(scripMasterDf['Exchange'] + '|' + scripMasterDf['TradingSymbol'], scripMasterDf['Exchange'] + '|' + scripMasterDf['Token'].astype(str)))
    tokenMappings['BSE|SENSEX'] = 'BSE|1'
    tokenMappings['BSE|BANKEX'] = 'BSE|12'
    return tokenMappings

def getFeed(cred, registerOptions, underlyings):
    api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                        websocket='wss://api.shoonya.com/NorenWSTP/')
    userToken = None
    tokenFile = 'shoonyakey.txt'
    if os.path.exists(tokenFile) and (datetime.datetime.fromtimestamp(os.path.getmtime(tokenFile)).date() == datetime.datetime.today().date()):
        logger.info(f"Token has been created today already. Re-using it")
        with open(tokenFile, 'r') as f:
            userToken = f.read()
        logger.info(
            f"userid {cred['user']} password ******** usertoken {userToken}")
        loginStatus = api.set_session(
            userid=cred['user'], password=cred['pwd'], usertoken=userToken)
    else:
        logger.info(f"Logging in and persisting user token")
        loginStatus = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                                vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

        if loginStatus:
            with open(tokenFile, 'w') as f:
                f.write(loginStatus.get('susertoken'))

            logger.info(
                f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")
        else:
            logger.info(f'Login failed!')

    if loginStatus != None:
        if len(underlyings) == 0:
            underlyings = [underlying.replace(":","|") for underlying in getDefaultUnderlyings()]

        optionSymbols = []
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

                (currentWeeklyExpiry,nextWeekExpiry,monthlyExpiry) = getExpiryDates(index)

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

        logger.info('Creating feed object')
        return LiveTradeFeed(api, tokenMappings, underlyings + optionSymbols), api
    else:
        exit(1)

if __name__ == '__main__':
    print(getScriptMaster())
