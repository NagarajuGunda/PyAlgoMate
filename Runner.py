import yaml
from importlib import import_module
import threading
import logging
import signal
import zmq
import json
import traceback
import socket
from logging.handlers import SysLogHandler
from pyalgomate.telegram import TelegramBot
from pyalgomate.core import State
from pyalgomate.brokers import getFeed, getBroker
import log_setup  # noqa

logger = logging.getLogger(__file__)


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


context = zmq.Context()
sock = context.socket(zmq.PUB)


def valueChangedCallback(strategy, value):
    jsonDump = json.dumps({strategy: value})
    sock.send_json(jsonDump)


def main():
    with open("strategies.yaml", "r") as file:
        config = yaml.safe_load(file)

    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    telegramBot = None
    if 'Telegram' in creds and 'token' in creds['Telegram']:
        telegramBot = TelegramBot(
            creds['Telegram']['token'], creds['Telegram']['chatid'], creds['Telegram']['allow'])

    strategies = []

    if 'Streamlit' in config:
        port = config['Streamlit']['Port']
        sock.bind(f"tcp://127.0.0.1:{port}")

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
        _format = ('%(asctime)s [%(hostname)s] [%(processName)s:%(process)d] [%(threadName)s:%(thread)d] [%(name)s] [%('
                   'levelname)s] - %(message)s')
        _formatter = logging.Formatter(_format, datefmt='%b %d %H:%M:%S')
        syslog.setFormatter(_formatter)
        logger.addHandler(syslog)
        logger.setLevel(logging.INFO)

    feed, api = getFeed(
        creds, broker=config['Broker'], underlyings=config['Underlyings'])

    logger.info(f"Starting {config['Broker']} data feed....")
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
            strategyArgs.append({'callback': valueChangedCallback})

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

    threads = []

    for strategyObject in strategies:
        thread = threading.Thread(target=threadTarget, args=(strategyObject,))
        thread.start()
        threads.append(thread)

    if telegramBot:
        def handle_interrupt(signum, frame):
            logger.info("Ctrl+C received. Stopping the bot...")

            # Stop the strategies
            for _strategyObject in strategies:
                _strategyObject.stop()

            telegramBot.stop()

            # Stop the threads
            for _thread in threads:
                _thread.join()

            telegramBot.waitUntilFinished()
            telegramBot.delete()

            logger.info("Bot stopped. Exiting the process.")
            exit(0)

        signal.signal(signal.SIGINT, handle_interrupt)

    if telegramBot:
        telegramBot.waitUntilFinished()
    else:
        while any(thread.is_alive() for thread in threads):
            for thread in threads:
                if thread.is_alive():
                    thread.join(timeout=1)


if __name__ == "__main__":
    main()
