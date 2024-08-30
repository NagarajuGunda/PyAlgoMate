import os
import sys

sys.path.append(os.path.abspath(os.path.join(
os.path.dirname(os.path.abspath(__file__)), os.pardir)))

from pyalgomate.core import State
from pyalgomate.brokers import getFeed, getBroker
from pyalgomate.telegram import TelegramBot
from pyalgomate.ui.flet.views.trades import TradesView
from pyalgomate.ui.flet.views.strategies import StrategiesView
import pyalgotrade
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
from flet_core.page import PageDisconnectedException
import sys
import sentry_sdk

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

    _feed, api = getFeed(creds, config, underlyings=config['Underlyings'])

    _feed.start()

    if 'Strategies' not in config or config['Strategies'] is None:
        return _feed, strategies

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
    def handleOrderEvent(strategy, broker, orderEvent):
        if orderEvent.getEventType() not in (pyalgotrade.broker.OrderEvent.Type.PARTIALLY_FILLED, pyalgotrade.broker.OrderEvent.Type.FILLED):
            return

        with lock:
            topView = None
            try:
                if len(page.views):
                    topView = page.views[-1]

                    if hasattr(topView, 'reload'):
                        topView.reload()
            except Exception as e:
                logger.error(f"Error updating views: {str(e)}")
                logger.exception("Exception details:")

    for strategy in strategies:
        strategy.getBroker().getOrderUpdatedEvent().subscribe(
            lambda broker, orderEvent, s=strategy: handleOrderEvent(
                s, broker, orderEvent)
        )

    page.horizontal_alignment = "center"
    page.vertical_alignment = "center"
    page.padding = ft.padding.only(left=50, right=50)
    page.scroll = ft.ScrollMode.HIDDEN
    lock = threading.Lock()

    strategiesView = StrategiesView(page, _feed, strategies)

    def route_change(route):
        with lock:
            try:
                route = urlparse(route.route)
                params = parse_qs(route.query)
                page.views.clear()

                page.views.append(strategiesView)
                if route.path == "/trades":
                    strategyName = params['strategyName'][0]
                    strategy = next(
                        (s for s in strategies if s.strategyName == strategyName), None)
                    if strategy:
                        page.views.append(TradesView(page, strategy))
                elif route.path == '/strategy':
                    strategyName = params['strategyName'][0]
                    strategy = next(
                        (s for s in strategies if s.strategyName == strategyName), None)
                    if strategy:
                        page.views.append(strategy.getView(page))
                page.update()
            except PageDisconnectedException:
                logger.warning("Page disconnected during route change.")
            except Exception as e:
                logger.error(f"Error in route_change: {str(e)}")
                logger.exception("Exception details:")

    def view_pop(view: ft.View):
        with lock:
            try:
                page.views.pop()
                page.update()
            except PageDisconnectedException:
                logger.warning("Page disconnected during view pop.")
            except Exception as e:
                logger.error(f"Error in view_pop: {str(e)}")
                logger.exception("Exception details:")

    page.on_view_pop = view_pop
    page.views.append(strategiesView)

    def update_views():
        while True:
            with lock:
                try:
                    if len(page.views):
                        topView = page.views[-1]
                        if (
                            topView in page.views
                        ):  # Check if the view is still in the page
                            try:
                                if hasattr(topView, "updateData"):
                                    topView.updateData()
                                elif hasattr(topView, "update"):
                                    topView.update()
                            except Exception:
                                pass
                except PageDisconnectedException:
                    logger.warning(
                        "Page disconnected during view update. Will retry on next iteration.")
                except Exception:
                    pass
            time.sleep(1)

    update_thread = threading.Thread(target=update_views, daemon=True)
    update_thread.start()

    def on_disconnect(e):
        logger.warning(f"Page disconnected. Details {e}")

    page.on_disconnect = on_disconnect


if __name__ == "__main__":
    fletPath = os.getenv("FLET_PATH", '')
    fletPort = int(os.getenv("FLET_PORT", '8502'))
    fletView = os.getenv("FLET_VIEW", ft.WEB_BROWSER)
    if fletView != ft.FLET_APP:
        fletView = None
    ft.app(name=fletPath, target=main, view=fletView, port=fletPort)
