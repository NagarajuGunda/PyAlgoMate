"""
.. moduleauthor:: Nagaraju Gunda
"""

import logging
import yaml
import datetime
import pandas as pd
import io
import requests
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))), os.pardir))


def sendToTelegram(botToken, chatId, topicId, data: pd.DataFrame, fileName):
    if data is None or data.empty:
        return

    url = f'https://api.telegram.org/bot{botToken}/sendDocument'

    parquetBuffer = io.BytesIO()
    data.to_parquet(parquetBuffer)
    parquetBuffer.seek(0)

    response = requests.post(url, data={'chat_id': chatId, 'message_thread_id': topicId}, files={
        'document': (fileName, parquetBuffer, 'application/octet-stream')}, verify=False)
    if response.status_code == 200:
        logger.info(f'{fileName} sent successfully!')
    else:
        logger.error(
            f'Failed to send {fileName}. Status code: {response.status_code}')


if __name__ == "__main__":
    import pyalgomate.brokers.finvasia as finvasia
    import pyalgomate.brokers.finvasia.broker as broker
    from pyalgomate.brokers.finvasia.broker import underlyingMapping
    import pyalgomate.utils as utils

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(levelname)-5s]|[%(asctime)s]|[PID:%(process)d::TID:%(thread)d]|[%(name)s::%(module)s::%(funcName)s::%("
        "lineno)d]|=> %(message)s"
    )

    fileHandler = logging.FileHandler('DataDownloader.log', 'a', 'utf-8')
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(formatter)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(formatter)

    logger.addHandler(fileHandler)
    logger.addHandler(consoleHandler)

    logging.getLogger("requests").setLevel(logging.WARNING)

    creds = None
    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    api = finvasia.getApi(creds['Finvasia'])
    data: pd.DataFrame = None
    today = datetime.datetime.now()
    fromTime = datetime.datetime(
        year=today.year, month=today.month, day=today.day, hour=0, minute=0)

    for index in underlyingMapping.keys():
        historicalData = broker.getHistoricalData(
            api, index, fromTime, '1')
        historicalData['Ticker'] = str(underlyingMapping[index]['index'])
        if data is not None:
            data = pd.concat([data, historicalData])
        else:
            data = historicalData

    if 'Telegram' not in creds:
        exit(0)

    botToken = creds['Telegram']['token']
    chatId = '@pyalgomate'
    topicId = '116736'

    sendToTelegram(botToken, chatId, topicId, data,
                   f"IndexData-{today.strftime('%Y-%m-%d')}.parquet")

    scripMasterDf: pd.DataFrame = finvasia.getScriptMaster()
    scripMasterDf['Expiry'] = pd.to_datetime(
        scripMasterDf['Expiry'], format='%d-%b-%Y')

    for index in underlyingMapping.keys():
        expiry = utils.expiry.getNearestWeeklyExpiryDate(
            today, underlyingMapping[index]['index'])
        if expiry == today.date():
            data: pd.DataFrame = None
            filteredScripMasterDf = scripMasterDf[(scripMasterDf['Expiry'].dt.date == expiry) &
                                                  (scripMasterDf['Instrument'] == 'OPTIDX')]
            for idx, row in filteredScripMasterDf.iterrows():
                historicalData = broker.getHistoricalData(
                    api, row['Exchange'] + '|' + row['TradingSymbol'], fromTime - datetime.timedelta(days=40), '1')
                historicalData['Ticker'] = row['TradingSymbol']
                if data is not None:
                    data = pd.concat([data, historicalData])
                else:
                    data = historicalData
            data = data.sort_values(['Ticker', 'Date/Time']).drop_duplicates(
                subset=['Ticker', 'Date/Time'], keep='first')
            sendToTelegram(botToken, chatId, topicId, data,
                           f"{str(underlyingMapping[index]['index'])}-{today.strftime('%Y-%m-%d')}.parquet")
