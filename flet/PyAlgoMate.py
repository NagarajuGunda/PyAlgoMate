import os
import yaml
import logging
import threading
import traceback
import socket
from logging.handlers import SysLogHandler
from importlib import import_module
from typing import List

import flet as ft

from pyalgomate.telegram import TelegramBot
from pyalgomate.brokers import getFeed, getBroker
from pyalgomate.core import State
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
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
    def __init__(self, strategy: BaseOptionsGreeksStrategy, page: ft.Page):
        super().__init__()

        self.strategy = strategy
        self.page = page
        self.expand = True
        self.stateText = ft.Text(
            self.strategy.state,
            size=20
        )
        self.pnlText = ft.Text(
            "₹ 0",
            size=25
        )

        self.openPositions = ft.Text(
            'Open Pos: 0',
            size=12
        )
        self.closedPositions = ft.Text(
            'Closed Pos: 0',
            size=12
        )

        self.closeDialogModel = ft.AlertDialog(
            modal=True,
            title=ft.Text("Please Confirm"),
            content=ft.Text("Do you really want to square off all positions?"),
            actions=[
                ft.TextButton("Yes", on_click=self.squareOff),
                ft.TextButton("No", on_click=self.closeDialog),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            on_dismiss=lambda e: print("Modal dialog dismissed!"),
        )

        self.content = ft.Row(
            height=100,
            controls=[
                ft.Container(
                    ft.Column(
                        [
                            ft.Row(
                                [
                                    ft.Text(
                                        strategy.strategyName,
                                        size=15,
                                        weight='w700'
                                    ),
                                    ft.Text(
                                        strategy.getBroker().getType(),
                                        size=10
                                    )
                                ],
                                spacing=10),
                            ft.Row(
                                [
                                    self.openPositions,
                                    self.closedPositions
                                ]
                            )
                        ],
                        alignment=ft.MainAxisAlignment.CENTER
                    ),
                    expand=1,
                    padding=ft.padding.only(left=50)
                ),
                ft.Container(
                    ft.Column(
                        [self.stateText],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER
                    ),
                    expand=1
                ),
                ft.Container(
                    ft.Column(
                        [self.pnlText],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER
                    ),
                    expand=1
                ),
                ft.Container(
                    ft.Column([ft.ElevatedButton(
                        text='Trades',
                        color='white',
                        bgcolor='#263F6A'
                    )],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        expand=1
                    ),
                    expand=1
                ),
                ft.Container(
                    ft.Column([
                        ft.TextButton("Square Off", icon="close_rounded", icon_color="red400",
                                      on_click=self.openDialog)
                    ],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        expand=1
                    ),
                    expand=1
                )
            ]
        )

    def onBars(self):
        self.stateText.value = str(self.strategy.state)
        pnl = self.strategy.getOverallPnL()
        self.pnlText.value = f'₹ {pnl:.2f}'
        self.pnlText.color = "green" if pnl >= 0 else "red"
        self.openPositions.value = f'Open Pos: {len(self.strategy.openPositions)}'
        self.closedPositions.value = f'Closed Pos: {len(self.strategy.closedPositions)}'
        self.update()

    def squareOff(self, e):
        self.closeDialogModel.open = False
        self.strategy.state = State.PLACING_ORDERS
        self.strategy.closeAllPositions()
        self.page.snack_bar = ft.SnackBar(
            ft.Row([ft.Text(f"Closing all positions !!!", size=20)],
                   alignment='center'),
            bgcolor='#263F6A'
        )
        self.page.snack_bar.open = True
        self.page.update()

    def closeDialog(self, e):
        self.closeDialogModel.open = False
        self.page.update()

    def openDialog(self, e):
        self.page.dialog = self.closeDialogModel
        self.closeDialogModel.open = True
        self.page.update()


class StrategiesContainer(ft.Container):
    def __init__(self, page: ft.Page, feed, strategies: List[BaseOptionsGreeksStrategy]):
        super().__init__()
        self.padding = ft.padding.only(top=20)
        self.strategies = strategies
        self.page = page

        feed.getNewValuesEvent().subscribe(self.onBars)

        self.totalMtm = ft.Text(
            '₹ 0', size=25)

        totalMtmRow = ft.Container(
            ft.Column([
                ft.Container(ft.Text('Total MTM', size=15, weight=ft.FontWeight.BOLD,
                             color='white'), padding=ft.padding.only(top=10, left=10)),
                ft.Container(self.totalMtm, padding=ft.padding.only(left=10)),
                ft.Container(
                    ft.Column([
                        ft.Divider(color='white'),
                        ft.Text('MTM Graph  -->', size=15,
                                weight=ft.FontWeight.W_400, color='white')
                    ]),
                    padding=ft.padding.only(
                        top=35, left=10, right=10, bottom=10)
                )
            ]),
            width=200,
            height=200,
            bgcolor='#263F6A',
            border_radius=10
        )

        self.strategyCards = [StrategyCard(
            strategy, page) for strategy in self.strategies]
        rows = [totalMtmRow]
        rows.extend([ft.Row([strategyCard])
                    for strategyCard in self.strategyCards])
        self.content = ft.Column(
            rows
        )

    def onBars(self, dateTime, bars):
        for strategyCard in self.strategyCards:
            strategyCard.onBars()

        totalMtm = sum([strategy.getOverallPnL()
                       for strategy in self.strategies])
        self.totalMtm.value = f'₹ {totalMtm:.2f}'
        self.totalMtm.color = "green" if totalMtm >= 0 else "red"
        self.update()


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
    thread = threading.Thread(target=threadTarget, args=(strategyObject,))
    thread.daemon = True
    thread.start()
    threads.append(thread)


def main(page: ft.Page):
    page.horizontal_alignment = "center"
    page.vertical_alignment = "center"
    page.padding = ft.padding.only(left=50, right=50)
    page.bgcolor = "#212328"

    t = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        label_color='white90',
        unselected_label_color='white54',
        tabs=[
            ft.Tab(
                text="Strategies",
                content=StrategiesContainer(
                    page=page, feed=feed, strategies=strategies),
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
    fletPath = os.getenv("FLET_PATH", '')
    fletPort = int(os.getenv("FLET_PORT", '8502'))
    fletView = os.getenv("FLET_VIEW", ft.AppView.FLET_APP)
    try:
        fletView = ft.AppView(fletView)
    except Exception as e:
        fletView = None
    ft.app(name=fletPath, target=main, view=fletView, port=fletPort)
