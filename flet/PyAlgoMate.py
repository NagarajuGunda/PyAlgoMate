import os
import yaml
import logging
import traceback
import socket
import time
import multiprocessing

from logging.handlers import SysLogHandler
from importlib import import_module

import flet as ft
from components import StrategiesContainer, LoggingControl
from pyalgomate.telegram import TelegramBot
from pyalgomate.brokers import getFeed, getBroker
from pyalgomate.core import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logging.getLogger("flet").setLevel(logging.DEBUG)
logging.getLogger("flet_core").setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fileHandler = logging.FileHandler('PyAlgoMate.log')
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(formatter)

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(formatter)

logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)

logging.getLogger("requests").setLevel(logging.WARNING)

def GetFeedNStrategies(creds):
    with open("strategies.yaml", "r") as file:
        config = yaml.safe_load(file)

    telegramBot = None
    if 'Telegram' in creds and 'token' in creds['Telegram']:
        telegramBot = TelegramBot(
            creds['Telegram']['token'], creds['Telegram']['chatid'], creds['Telegram']['allow'])

    strategies = []

    feed, api = getFeed(
        creds, broker=config['Broker'], underlyings=config['Underlyings'])

    feed.start()

    for strategyName, details in config['Strategies'].items():
        try:
            strategyClassName = details['Class']
            strategyPath = details['Path']
            strategyMode = details['Mode']
            strategyArgs = details['Args'] if details['Args'] is not None else list(
            )
            strategyArgs.append({'telegramBot': telegramBot})
            strategyArgs.append({'strategyName': strategyName})

            module = import_module(
                strategyPath.replace('.py', '').replace('/', '.'))
            strategyClass = getattr(module, strategyClassName)
            strategyArgsDict = {
                key: value for item in strategyArgs for key, value in item.items()}

            broker = getBroker(feed, api, config['Broker'], strategyMode)

            if hasattr(strategyClass, 'getAdditionalArgs') and callable(getattr(strategyClass, 'getAdditionalArgs')):
                additionalArgs = strategyClass.getAdditionalArgs(broker)

                if additionalArgs:
                    for key, value in additionalArgs.items():
                        if key not in strategyArgsDict:
                            strategyArgsDict[key] = value

            strategyInstance = strategyClass(
                feed=feed, broker=broker, **strategyArgsDict)

            strategies.append(strategyInstance)
        except Exception as e:
            logger.error(
                f'Error in creating strategy instance for <{strategyName}>. Error: {e}')
            logger.exception(traceback.format_exc())

    return feed, strategies


def runStrategy(strategy):
    try:
        strategy.run()
    except Exception as e:
        strategy.state = State.UNKNOWN
        logger.exception(
            f'Error occurred while running strategy {strategy.strategyName}. Exception: {e}')
        logger.exception(traceback.format_exc())


def threadTarget(strategy):
    try:
        runStrategy(strategy)
    except Exception as e:
        strategy.state = State.UNKNOWN
        logger.exception(
            f'An exception occurred in thread for strategy {strategy.strategyName}. Exception: {e}')
        logger.exception(traceback.format_exc())


creds = None
with open('cred.yml') as f:
    creds = yaml.load(f, Loader=yaml.FullLoader)

if 'PaperTrail' in creds:
    papertrailCreds = creds['PaperTrail']['address'].split(':')

    class ContextFilter(logging.Filter):
        hostname = socket.gethostname()

        def filter(self, record):
            record.hostname = ContextFilter.hostname
            return True

    syslog = SysLogHandler(
        address=(papertrailCreds[0], int(papertrailCreds[1])))
    syslog.addFilter(ContextFilter())
    format = '%(asctime)s [%(hostname)s] [%(processName)s:%(process)d] [%(threadName)s:%(thread)d] [%(name)s] [%(levelname)s] - %(message)s'
    formatter = logging.Formatter(format, datefmt='%b %d %H:%M:%S')
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)
    logger.setLevel(logging.INFO)

feed, strategies = GetFeedNStrategies(creds)

threads = []

for strategyObject in strategies:
    thread = multiprocessing.Process(target=threadTarget, args=(strategyObject,))
    thread.daemon = True
    thread.start()
    threads.append(thread)


def main(page: ft.Page):
    page.horizontal_alignment = "center"
    page.vertical_alignment = "center"
    page.padding = ft.padding.only(left=50, right=50)
    page.bgcolor = "#212328"

    strategiesContainer = StrategiesContainer(
        page=page, feed=feed, strategies=strategies)

    t = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        tabs=[
            ft.Tab(
                text="Strategies",
                content=strategiesContainer,
            ),
            ft.Tab(
                text="Trade Terminal",
                icon=ft.icons.TERMINAL,
                content=ft.Text("This is Tab 2"),
            )
        ],
        expand=1,
    )

    page.add(t)

    page.update()

    while True:
        strategiesContainer.updateStrategies()
        time.sleep(0.1)


if __name__ == "__main__":
    fletPath = os.getenv("FLET_PATH", '')
    fletPort = int(os.getenv("FLET_PORT", '8502'))
    fletView = os.getenv("FLET_VIEW", ft.FLET_APP)
    if fletView != ft.FLET_APP:
        fletView = None
    ft.app(name=fletPath, target=main, view=fletView, port=fletPort)
