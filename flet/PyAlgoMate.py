import yaml
import logging
import threading
import traceback
from importlib import import_module
from typing import List

import flet as ft

from pyalgomate.brokers import getFeed, getBroker
from pyalgomate.core import State
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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


class StrategyCard(ft.Card):
    def __init__(self, strategy: BaseOptionsGreeksStrategy):
        super().__init__()

        self.strategy = strategy

        self.stateText = ft.Text(
            self.strategy.state
        )
        self.pnlText = ft.Text(
            self.strategy.getOverallPnL()
        )

        self.content = ft.Row(
            height=100,
            controls=[
                ft.Column(
                    controls=[ft.Text(
                        strategy.strategyName
                    )]
                ),
                ft.Column(
                    controls=[
                        self.stateText
                    ]
                ),
                ft.Column(
                    controls=[
                        self.pnlText
                    ]
                ),
                ft.Column(
                    controls=[
                        ft.ElevatedButton(
                            text='Trades'
                        )
                    ]
                )
            ]
        )

        self.strategy.getFeed().getNewValuesEvent().subscribe(self.onBars)

    def onBars(self, dateTime, bars):
        self.stateText.value = str(self.strategy.state)
        self.pnlText.value = f'{self.strategy.getOverallPnL():.2f}'
        self.update()


class StrategiesContainer(ft.Container):
    def __init__(self, strategies: List[BaseOptionsGreeksStrategy]):
        super().__init__()

        self.strategies = strategies
        self.content = ft.Column(
            controls=[
                ft.Row(
                    controls=[
                        StrategyCard(strategy)]
                ) for strategy in strategies
            ]
        )


def GetStrategies():
    with open("strategies.yaml", "r") as file:
        config = yaml.safe_load(file)

    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    strategies = []

    feed, api = getFeed(
        creds, broker='Backtest', underlyings=config['Underlyings'])

    feed.start()
    telegramBot = None

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

    return strategies


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


def main(page: ft.Page):
    page.horizontal_alignment = "center"
    page.vertical_alignment = "center"
    page.padding = ft.padding.only(right=50)
    page.bgcolor = "#212328"

    strategies = GetStrategies()
    t = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        label_color='white90',
        unselected_label_color='white54',
        tabs=[
            ft.Tab(
                text="Strategies",
                content=StrategiesContainer(strategies),
            ),
            ft.Tab(
                text="Trade Terminal",
                icon=ft.icons.TERMINAL,
                content=ft.Text("This is Tab 2"),
            ),
        ],
        expand=1,
    )

    page.add(t)

    page.update()

    threads = []

    for strategyObject in strategies:
        thread = threading.Thread(target=threadTarget, args=(strategyObject,))
        thread.daemon = True
        thread.start()
        threads.append(thread)


if __name__ == "__main__":
    ft.app(target=main, view=ft.AppView.WEB_BROWSER)
