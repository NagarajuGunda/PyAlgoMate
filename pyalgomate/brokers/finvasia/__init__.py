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
from concurrent.futures import ThreadPoolExecutor

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

def getTokenMappings() -> dict:
    scripMasterDf: pd.DataFrame = getScriptMaster()
    tokenMappings = dict(zip(scripMasterDf['Exchange'] + '|' + scripMasterDf['TradingSymbol'], scripMasterDf['Exchange'] + '|' + scripMasterDf['Token'].astype(str)))
    tokenMappings['BSE|SENSEX'] = 'BSE|1'
    tokenMappings['BSE|BANKEX'] = 'BSE|12'
    return tokenMappings

if __name__ == '__main__':
    print(getScriptMaster())
