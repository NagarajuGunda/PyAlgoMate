import os
import yaml
import logging
import threading
import traceback
import socket
import time
from urllib.parse import parse_qs, urlparse
from logging.handlers import SysLogHandler
from importlib import import_module
import flet as ft
import sys
import sentry_sdk

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)))

from views.strategies import StrategiesView
from views.trades import TradesView
from pyalgomate.telegram import TelegramBot
from pyalgomate.brokers import getFeed, getBroker
from pyalgomate.core import State

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "[%(levelname)-5s]|[%(asctime)s]|[PID:%(process)d::TID:%(thread)d]|[%(name)s::%(module)s::%(funcName)s::%("
    "lineno)d]|=> %(message)s"
)

fileHandler = logging.FileHandler('PyAlgoMate.log', 'a', 'utf-8')
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

    _feed, api = getFeed(creds, broker=config['Broker'], underlyings=config['Underlyings'])

    _feed.start()

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

            broker = getBroker(_feed, api, config['Broker'], strategyMode)

            if hasattr(strategyClass, 'getAdditionalArgs') and callable(getattr(strategyClass, 'getAdditionalArgs')):
                additionalArgs = strategyClass.getAdditionalArgs(broker)

                if additionalArgs:
                    for key, value in additionalArgs.items():
                        if key not in strategyArgsDict:
                            strategyArgsDict[key] = value

            strategyInstance = strategyClass(
                feed=_feed, broker=broker, **strategyArgsDict)

            strategies.append(strategyInstance)
        except Exception as e:
            logger.error(
                f'Error in creating strategy instance for <{strategyName}>. Error: {e}')
            logger.exception(traceback.format_exc())

    return _feed, strategies


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

sentry_dns = creds.get('SENTRY', {}).get('SENTRY_DNS')
env_local = creds.get('ENV', {}).get('LOCAL')
prod_provider = creds.get('ENV', {}).get('PROD_PROVIDER')

if 'PaperTrail' in creds:
    papertrailCreds = creds['PaperTrail']['address'].split(':')

if sentry_dns and env_local == 'True':
    sentry_sdk.init(sentry_dns, server_name=prod_provider)

    class ContextFilter(logging.Filter):
        hostname = socket.gethostname()

        def filter(self, record):
            record.hostname = ContextFilter.hostname
            return True


    syslog = SysLogHandler(
        address=(papertrailCreds[0], int(papertrailCreds[1])))
    syslog.addFilter(ContextFilter())
    format = ('%(asctime)s [%(hostname)s] [%(processName)s:%(process)d] [%(threadName)s:%(thread)d] [%(name)s] [%('
              'levelname)s] - %(message)s')
    formatter = logging.Formatter(format, datefmt='%b %d %H:%M:%S')
    syslog.setFormatter(formatter)
    logger.addHandler(syslog)
    logger.setLevel(logging.INFO)

_feed, strategies = GetFeedNStrategies(creds)

threads = []

for strategyObject in strategies:
    thread = threading.Thread(target=threadTarget, args=(strategyObject,))
    thread.daemon = True
    thread.start()
    threads.append(thread)


def main(page: ft.Page):
    page.horizontal_alignment = "center"
    page.vertical_alignment = "center"
    page.padding = ft.padding.only(left=50, right=50)
    page.scroll = ft.ScrollMode.HIDDEN
    lock = threading.Lock()

    strategiesView = StrategiesView(page, _feed, strategies)

    def route_change(route):
        with lock:
            route = urlparse(route.route)
            params = parse_qs(route.query)
            page.views.clear()

            page.views.append(
                strategiesView
            )
            if route.path == "/trades":
                strategyName = params['strategyName'][0]
                strategy = [strategy for strategy in strategies if strategy.strategyName == strategyName][0]
                page.views.append(
                    TradesView(page, strategy)
                )
            elif route.path == '/strategy':
                strategyName = params['strategyName'][0]
                strategy = [strategy for strategy in strategies if strategy.strategyName == strategyName][0]
                page.views.append(strategy.getView(page))
            page.update()

    def view_pop(view: ft.View):
        page.views.pop()
        top_view = page.views[-1]
        page.go(top_view.route)

    page.on_route_change = route_change
    page.on_view_pop = view_pop
    page.go(page.route)

    while True:
        with lock:
            if len(page.views):
                topView = page.views[-1]
                if topView in page.views:
                    topView.update()
        time.sleep(0.5)


if __name__ == "__main__":
    fletPath = os.getenv("FLET_PATH", '')
    fletPort = int(os.getenv("FLET_PORT", '8502'))
    fletView = os.getenv("FLET_VIEW", ft.FLET_APP)
    if fletView != ft.FLET_APP:
        fletView = None
    ft.app(name=fletPath, target=main, view=fletView, port=fletPort)
