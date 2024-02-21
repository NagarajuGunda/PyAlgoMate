import base64
import logging
import json
from flet_core.control import OptionalNumber
from flet_core.ref import Ref
from flet_core.types import ResponsiveNumber
import pandas as pd
import flet as ft
from typing import Any, List, Optional, Union

from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.core import State


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
        self.balanceAvailable = ft.Text(
            f'Balance Available: ₹ {strategy.getBroker().getCash()}',
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

        self.content = ft.ResponsiveRow(
            [
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
                                    ft.Column(
                                        [self.openPositions, self.closedPositions]
                                    )
                                ],
                            ),
                            ft.Row(
                                [
                                    self.balanceAvailable
                                ]
                            )
                        ],
                        alignment=ft.MainAxisAlignment.CENTER
                    ),
                    margin=ft.margin.only(top=20, bottom=20, left=20),
                    col={"sm": 12, "md": 2},
                ),
                ft.Container(
                    ft.Column(
                        [self.stateText],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER
                    ),
                    #expand=2
                    col={"sm": 12, "md": 2},
                ),
                ft.Container(
                    ft.Column(
                        [self.pnlText],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER
                    ),
                    col={"sm": 12, "md": 2},
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
                    col={"sm": 12, "md": 2},
                ),
                ft.Container(
                    ft.Column([ft.IconButton(
                        icon=ft.icons.INSERT_CHART_ROUNDED,
                        icon_size=40,
                        icon_color='#263F6A',
                        on_click=self.onChartButtonClicked
                    )],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        expand=1
                    ),
                    col={"sm": 12, "md": 1},
                ),
                ft.Container(
                    ft.Column([ft.IconButton(
                        icon=ft.icons.INFO_ROUNDED,
                        icon_size=40,
                        icon_color='#263F6A',
                        on_click=self.onInfoButtonClicked
                    )],
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        expand=1
                    ),
                    col={"sm": 12, "md": 1},
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
                    col={"sm": 12, "md": 2},
                )
            ]
        )

    def updateStrategy(self):
        self.stateText.value = str(self.strategy.state)
        pnl = self.strategy.getOverallPnL()
        self.pnlText.value = f'₹ {pnl:.2f}'
        self.pnlText.color = "green" if pnl >= 0 else "red"
        activeBuyPositions = len([pos for pos in self.strategy.getActivePositions() if pos.getEntryOrder() and pos.getEntryOrder().isBuy()])
        activeSellPositions = len([pos for pos in self.strategy.getActivePositions() if pos.getEntryOrder() and (not pos.getEntryOrder().isBuy())])
        self.openPositions.value = f'Open Pos (B|S): {len(self.strategy.getActivePositions())} ({activeBuyPositions}|{activeSellPositions})'
        self.closedPositions.value = f'Closed Pos: {len(self.strategy.getClosedPositions())}'
        self.balanceAvailable.value = f'Balance Available: ₹ {self.strategy.getBroker().getCash()}'
        self.update()

    def onChartButtonClicked(self, e):
        base64Img = base64.b64encode(
            self.strategy.getPnLImage()).decode('utf-8')
        dlg = ft.AlertDialog(
            content=ft.Container(
                ft.Image(src_base64=base64Img)
            )
        )
        self.page.dialog = dlg
        dlg.open = True
        self.page.update()

    def onInfoButtonClicked(self, e):
        class MyEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, pd.Timestamp):
                    return obj.isoformat()
                return super().default(obj)

        variablesAndValues = vars(self.strategy)

        for key, value in variablesAndValues.items():
            if isinstance(value, pd.Timestamp):
                variablesAndValues[key] = str(value)

        def serialize(obj):
            try:
                return json.dumps(obj)
            except TypeError:
                return str(obj)

        serializableVars = {k: serialize(v)
                            for k, v in variablesAndValues.items()}

        # Convert DataFrames to list of dictionaries
        for key, value in serializableVars.items():
            if isinstance(variablesAndValues[key], pd.DataFrame):
                serializableVars[key] = variablesAndValues[key].to_dict(
                    orient='records')

        # Pretty print the result using json.dumps
        formattedText = json.dumps(serializableVars, indent=4, cls=MyEncoder)

        dlg = ft.AlertDialog(
            content=ft.Container(
                ft.Column(
                    [ft.Text(formattedText)],
                    scroll=ft.ScrollMode.AUTO
                )
            )
        )
        self.page.dialog = dlg
        dlg.open = True
        self.page.update()

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
    def __init__(self, page: ft.Page, feed: BaseBarFeed, strategies: List[BaseOptionsGreeksStrategy]):
        super().__init__()
        self.padding = ft.padding.only(top=20)
        self.strategies = strategies
        self.page = page
        self.feed = feed

        self.totalMtm = ft.Text(
            '₹ 0', size=25)

        self.feedIcon = ft.Icon(name=ft.icons.CIRCLE_ROUNDED)
        self.feedText = ft.Text(
            'Timestamps', size=10, italic=True)
        feedRow = ft.Container(
            ft.Container(
                ft.Column(
                    [
                        ft.Container(ft.Row([
                            ft.Text('Feed', size=15, weight='w700'),
                            self.feedIcon,
                        ])),
                        self.feedText
                    ],
                ),
                padding=ft.padding.all(20),
                width=300,
                bgcolor='white54',
                border_radius=10,
            ),
            alignment=ft.alignment.top_right,
        )
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
        rows = [feedRow, totalMtmRow]
        rows.append(ft.ListView([ft.Row([strategyCard])
                    for strategyCard in self.strategyCards]))
        self.content = ft.Column(
            rows,
            scroll=ft.ScrollMode.HIDDEN
        )

    def updateStrategies(self):
        for strategyCard in self.strategyCards:
            strategyCard.updateStrategy()

        totalMtm = sum([strategy.getOverallPnL()
                       for strategy in self.strategies])
        self.totalMtm.value = f'₹ {totalMtm:.2f}'
        self.totalMtm.color = "green" if totalMtm >= 0 else "red"
        self.feedIcon.color = "green" if self.feed.isDataFeedAlive() else "red"
        self.feedText.value = f'Quote       : {self.feed.getLastUpdatedDateTime()}\nReceived  : {self.feed.getLastReceivedDateTime()}\nBars        : {self.feed.getNextBarsDateTime()}'
        self.update()


class CallbackHandler(logging.Handler):
    def __init__(self, callback):
        super(CallbackHandler, self).__init__()
        self.callback = callback

    def emit(self, record):
        log_message = self.format(record)
        self.callback(log_message)


class LoggingControl(ft.UserControl):
    def __init__(self, logger):
        super().__init__()
        logger.addHandler(CallbackHandler(self.logCallback))
        self.list = ft.ListView(expand=True, spacing=0, auto_scroll=True)
        self.canUpdate = False

    def setCanUpdate(self):
        self.canUpdate = True

    def logCallback(self, message):
        self.list.controls.append(ft.Text(message,
                                  color=ft.colors.WHITE,
                                  selectable=True,
                                  font_family="Consolas"))

        if self.canUpdate:
            self.update()

    def build(self):
        return ft.Container(self.list, expand=True, bgcolor='black')
