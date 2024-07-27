"""
.. moduleauthor:: Nagaraju Gunda
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))), os.pardir))

import re
import pyalgomate.utils as utils
from pyalgomate.brokers.finvasia.broker import underlyingMapping
import pyalgomate.brokers.finvasia.broker as broker
import pyalgomate.brokers.finvasia as finvasia
import pyarrow.parquet as pq
import pyarrow as pa
import requests
import io
import pandas as pd
import datetime
import yaml
import logging


def sendToTelegram(botToken, chatId, topicId, data, fileName, isParquet=True):
    if data is None or len(data) == 0:
        logger.warning(f"No data to send for file: {fileName}")
        return

    url = f'https://api.telegram.org/bot{botToken}/sendDocument'

    try:
        buffer = io.BytesIO()
        if isParquet:
            table = pa.Table.from_pandas(data)
            pq.write_table(table, buffer)
        else:
            data.to_csv(buffer, index=False)
        buffer.seek(0)

        files = {'document': (fileName, buffer, 'application/octet-stream')}
        response = requests.post(url, data={
                                 'chat_id': chatId, 'message_thread_id': topicId}, files=files, verify=False)

        if response.status_code == 200:
            logger.info(f'{fileName} sent successfully!')
        else:
            logger.error(
                f'Failed to send {fileName}. Status code: {response.status_code}')
    except Exception as e:
        logger.exception(
            f"An error occurred while sending {fileName}: {str(e)}")


def process_historical_data(api, index, fromTime, toTelegram=True):
    historicalData = broker.getHistoricalData(api, index, fromTime, '1')
    historicalData['Ticker'] = str(underlyingMapping[index]['index'])

    if toTelegram:
        sendToTelegram(botToken, chatId, topicId, historicalData,
                       f"{str(underlyingMapping[index]['index'])}-{datetime.datetime.now().strftime('%Y-%m-%d')}.parquet")

    return historicalData


def transform_trading_symbol(expiry, optionPrefix, tradingSymbol):
    m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d+)([CP])E", tradingSymbol)
    if m is not None:
        optionPrefix = m.group(1)
        year = int(m.group(2)) + 2000
        return f"{optionPrefix}{expiry.day:02d}{expiry.strftime('%b').upper()}{year % 100:02d}{m.group(5)}{m.group(4)}"

    m = re.match(
        r"([A-Z\|]+)(\d{2})(\d|[OND])(\d{2})(\d+)([CP])E", tradingSymbol)
    if m is not None:
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
        optionPrefix = m.group(1)
        return f"{optionPrefix}{day:02d}{datetime.datetime(year, month, day).strftime('%b').upper()}{year % 100:02d}{m.group(6)}{m.group(5)}"

    return tradingSymbol


def process_options_data(api, index, fromTime, expiry, filteredScripMasterDf):
    data = process_historical_data(api, index, fromTime, False)

    for idx, row in filteredScripMasterDf.iterrows():
        historicalData = broker.getHistoricalData(
            api, row['Exchange'] + '|' + row['TradingSymbol'], fromTime, '1')

        historicalData['Ticker'] = row['TradingSymbol']
        data = pd.concat([data, historicalData])

        # Free up memory
        del historicalData

    data = data.sort_values(['Ticker', 'Date/Time']).drop_duplicates(
        subset=['Ticker', 'Date/Time'], keep='first')

    data['Ticker'] = data['Ticker'].apply(
        lambda x: transform_trading_symbol(expiry, underlyingMapping[index]['optionPrefix'], x))

    sendToTelegram(botToken, chatId, topicId, data,
                   f"{str(underlyingMapping[index]['index'])}-{datetime.datetime.now().strftime('%Y-%m-%d')}.parquet")

    # Free up memory
    del data


if __name__ == "__main__":
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
        creds = yaml.safe_load(f)

    api = finvasia.getApi(creds['Finvasia'])
    today = datetime.datetime.now()
    fromTime = datetime.datetime(
        year=today.year, month=today.month, day=today.day, hour=0, minute=0)

    if 'Telegram' not in creds:
        logger.fatal('Telegram creds not present!')
        exit(1)

    botToken = creds['Telegram']['token']
    chatId = '@pyalgomate'
    topicId = '116736'

    # Process index data
    index_data = pd.concat([process_historical_data(
        api, index, fromTime, False) for index in underlyingMapping.keys()])
    sendToTelegram(botToken, chatId, topicId, index_data,
                   f"IndexData-{today.strftime('%Y-%m-%d')}.parquet")
    del index_data  # Free up memory

    # Process script master
    scripMasterDf: pd.DataFrame = finvasia.getScriptMaster()
    scripMasterDf['Expiry'] = pd.to_datetime(
        scripMasterDf['Expiry'], format='%d-%b-%Y')
    sendToTelegram(botToken, chatId, topicId, scripMasterDf,
                   f"ScripMaster-{today.strftime('%Y-%m-%d')}.csv", isParquet=False)

    # Process options data
    for index in underlyingMapping.keys():
        indexName = underlyingMapping[index]['index']
        expiry = utils.expiry.getNearestWeeklyExpiryDate(
            today, indexName)
        if expiry == today.date():
            filteredScripMasterDf = scripMasterDf[(scripMasterDf['Expiry'].dt.date == expiry) &
                                                  (scripMasterDf['Instrument'] == 'OPTIDX') &
                                                  scripMasterDf['TradingSymbol'].str.startswith(str(indexName))]
            process_options_data(
                api, index, fromTime - datetime.timedelta(days=40), expiry, filteredScripMasterDf)

    # Free up memory
    del scripMasterDf
