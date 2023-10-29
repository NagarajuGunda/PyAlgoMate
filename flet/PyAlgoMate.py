import yaml
import logging
from importlib import import_module
from typing import List

import flet as ft

from pyalgomate.brokers import getFeed, getBroker
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
                        ft.Text(
                            strategy.state
                        )
                    ]
                ),
                ft.Column(
                    controls=[
                        ft.Text(
                            strategy.getOverallPnL()
                        )
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

    # feed.start()
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

                for key, value in additionalArgs.items():
                    if key not in strategyArgsDict:
                        strategyArgsDict[key] = value

            strategyInstance = strategyClass(
                feed=feed, broker=broker, **strategyArgsDict)

            strategies.append(strategyInstance)
        except Exception as e:
            logger.error(
                f'Error in creating strategy instance for <{strategyName}>. Error: {e}')

    return strategies


def main(page: ft.Page):
    page.horizontal_alignment = "center"
    page.vertical_alignment = "center"
    page.padding = ft.padding.only(right=50)
    page.bgcolor = "#212328"

    t = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        label_color='white90',
        unselected_label_color='white54',
        tabs=[
            ft.Tab(
                text="Strategies",
                content=StrategiesContainer(GetStrategies()),
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


if __name__ == "__main__":
    ft.app(target=main, view=ft.AppView.WEB_BROWSER)
