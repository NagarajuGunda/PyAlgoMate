import base64
import json
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import flet as ft
from typing import List
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.barfeed import BaseBarFeed
from pyalgomate.core import State


class StrategyCard(ft.Card):
    def __init__(self, strategy: BaseOptionsGreeksStrategy, page: ft.Page):
        super().__init__()

        self.strategy = strategy
        self.page = page
        self.expand = True
        self.stateDropdown = ft.Dropdown(
            options=[ft.dropdown.Option(str(state)) for state in State],
            value=str(self.strategy.state),
            on_change=self.changeStrategyState,
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

        self.content = ft.Container(
            ft.ResponsiveRow([
                ft.Column([
                    ft.Text(strategy.strategyName, size=15, weight='w700'),
                    ft.Text(strategy.getBroker().getType(), size=10),
                    self.openPositions,
                    self.closedPositions,
                    self.balanceAvailable
                ], col={"sm": 12, "md": 3}, alignment=ft.MainAxisAlignment.CENTER),
                ft.Column([
                    self.stateDropdown
                ], col={"sm": 12, "md": 2}, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
                ft.Column([
                    self.pnlText
                ], col={"sm": 12, "md": 2}, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
                ft.Column([
                    ft.IconButton(
                        icon=ft.icons.REFRESH,
                        icon_size=40,
                        icon_color='#263F6A',
                        on_click=self.resetStrategy
                    )
                ], col={"sm": 3, "md": 1}, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
                ft.Column([
                    ft.ElevatedButton(
                        text='Trades',
                        style=ft.ButtonStyle(
                            color='white',
                            bgcolor={'': '#263F6A'},
                            shape=ft.RoundedRectangleBorder(radius=8),
                        ),
                        on_click=lambda _: self.page.go(
                            "/trades", strategyName=self.strategy.strategyName)
                    )
                ], col={"sm": 3, "md": 1}, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
                ft.Column([
                    ft.IconButton(
                        icon=ft.icons.INSERT_CHART_ROUNDED,
                        icon_size=40,
                        icon_color='#263F6A',
                        on_click=self.onChartButtonClicked
                    )
                ], col={"sm": 2, "md": 1}, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
                ft.Column([
                    ft.IconButton(
                        icon=ft.icons.INFO_ROUNDED,
                        icon_size=40,
                        icon_color='#263F6A',
                        on_click=self.onInfoButtonClicked
                    )
                ], col={"sm": 2, "md": 1}, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
                ft.Column([
                    ft.ElevatedButton(
                        "Exit",
                        icon=ft.icons.CLOSE_ROUNDED,
                        style=ft.ButtonStyle(
                            color={'': 'white'},
                            bgcolor={'': 'red400'},
                            shape=ft.RoundedRectangleBorder(radius=8),
                        ),
                        on_click=self.openDialog
                    )
                ], col={"sm": 2, "md": 1}, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            padding=10
        )

    def changeStrategyState(self, e):
        self.strategy.state = State[e.control.value]
        self.stateDropdown.value = str(self.strategy.state)
        self.stateDropdown.update()

    def resetStrategy(self, e):
        if hasattr(self.strategy, 'reset') and callable(self.strategy.reset):
            self.strategy.reset()
            self.updateStrategy()
            self.page.snack_bar = ft.SnackBar(
                ft.Row([ft.Text(f"Strategy {self.strategy.strategyName} has been reset.", size=20)],
                       alignment='center'),
                bgcolor='#263F6A'
            )
            self.page.snack_bar.open = True
        else:
            self.page.snack_bar = ft.SnackBar(
                ft.Row([ft.Text(f"Reset method not available for {self.strategy.strategyName}.", size=20)],
                       alignment='center'),
                bgcolor='#FF0000'
            )
            self.page.snack_bar.open = True
        self.page.update()

    def updateStrategy(self):
        if self.page is None:
            # Log an error message
            print("Error: `page` is None in `updateStrategy`")
            return

        self.stateDropdown.value = str(self.strategy.state)
        pnl = self.strategy.getOverallPnL()
        self.pnlText.value = f'₹ {pnl:.2f}'
        self.pnlText.color = "green" if pnl >= 0 else "red"
        activeBuyPositions = len([pos for pos in self.strategy.getActivePositions(
        ) if pos.getEntryOrder() and pos.getEntryOrder().isBuy()])
        activeSellPositions = len([pos for pos in self.strategy.getActivePositions(
        ) if pos.getEntryOrder() and (not pos.getEntryOrder().isBuy())])
        self.openPositions.value = f'Open Pos (B|S): {len(self.strategy.getActivePositions())} ({activeBuyPositions}|{activeSellPositions})'
        self.closedPositions.value = f'Closed Pos: {len(self.strategy.getClosedPositions())}'
        self.balanceAvailable.value = f'Balance Available: ₹ {self.strategy.getBroker().getCash()}'
        self.page.update()

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


class StrategiesView(ft.View):
    def __init__(self, page: ft.Page, feed: BaseBarFeed, strategies: List[BaseOptionsGreeksStrategy]):
        super().__init__()
        self.route = "/"
        self.padding = ft.padding.all(10)
        self.scroll = ft.ScrollMode.HIDDEN
        self.page = page
        self.strategies = strategies
        self.feed = feed

        self.totalMtm = ft.Text(
            '₹ 0', size=25)

        totalMtmRow = ft.Container(
            ft.Column([
                ft.Container(ft.Text('Total MTM', size=15, weight=ft.FontWeight.BOLD,
                             color='white'), padding=ft.padding.only(top=10, left=10)),
                ft.Container(self.totalMtm, padding=ft.padding.only(left=10)),
                ft.Column([
                    ft.Divider(color='white')
                ]),
                ft.Container(
                    ft.Row([
                        ft.Column([
                            ft.Text('Total MTM Graph ->', size=15,
                                    weight=ft.FontWeight.W_400, color='white')
                        ]),
                        ft.Column([
                            ft.IconButton(icon=ft.icons.INSERT_CHART_ROUNDED,
                                          icon_size=40,
                                          icon_color='white',
                                          on_click=self.onTotalMTMChartButtonClicked, tooltip="MTM of all Strategies running")
                        ])
                    ]),
                    padding=ft.padding.only(
                        top=35, right=10, bottom=10)
                )
            ]),
            col={"sm": 6, "md": 2},
            bgcolor='#263F6A',
            border_radius=10,
            height=200,
        )

        self.feedIcon = ft.Icon(name=ft.icons.CIRCLE_ROUNDED)
        self.feedText = ft.Text(
            'Timestamps', size=10, italic=True)
        feedRow = ft.Container(
            ft.Container(
                ft.Column(
                    [
                        ft.Row([
                            ft.Text('Feed', size=15, weight='w700'),
                            self.feedIcon,
                        ]),
                        self.feedText
                    ],
                ),
                padding=ft.padding.only(
                    top=35, left=20, right=20, bottom=20),
            ),
            col={"sm": 6, "md": 2},
            bgcolor='#ecf0f1',
            border_radius=10,
            height=200,
            alignment=ft.alignment.center,
        )

        self.strategyCards = [StrategyCard(
            strategy, page) for strategy in self.strategies]
        rows = [
            ft.ResponsiveRow([
                totalMtmRow, feedRow
            ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN
            )
        ]
        rows.append(ft.ListView([ft.Row([strategyCard])
                    for strategyCard in self.strategyCards]))
        self.controls = [
            ft.AppBar(title=ft.Text("Strategies"),
                      bgcolor=ft.colors.SURFACE_VARIANT),
            ft.Column(
                rows,
                scroll=ft.ScrollMode.HIDDEN
            )
        ]

    def onTotalMTMChartButtonClicked(self, e):
        pnlDf = pd.DataFrame()
        pnl = int(0)
        for strategyCard in self.strategyCards:
            strategy = strategyCard.strategy
            pnl = pnl + strategy.getOverallPnL()
            tempPnlDf = strategy.getPnLs()
            tempPnlDf['strategy'] = strategyCard.strategy.strategyName
            pnlDf = pd.concat([pnlDf, tempPnlDf], ignore_index=True)

        pnlDf.index = pnlDf['Date/Time']
        cummPnlDf = pnlDf['PnL'].resample('1T').agg({'PnL': 'sum'})
        cummPnlDf.reset_index(inplace=True)

        values = pd.to_numeric(cummPnlDf['PnL'])
        color = np.where(values < 0, 'loss', 'profit')
        fig = px.area(cummPnlDf, x="Date/Time", y=values, title=f"Total MTM | Current PnL:  ₹{round(pnl, 2)}",
                      color=color, color_discrete_map={'loss': 'orangered', 'profit': 'lightgreen'})
        fig.add_traces(
            [
                go.Scatter(x=pnlDf.query(f'strategy=="{strategy}"')["Date/Time"], y=pnlDf.query(f'strategy=="{strategy}"')['PnL'],
                           mode='lines',
                           name=f'{strategy}') for strategy in pnlDf.strategy.unique()
            ]
        )

        fig.update_layout(
            title_x=0.5, title_xanchor='center', yaxis_title='PnL')
        base64Img = base64.b64encode(
            fig.to_image(format='png')).decode('utf-8')
        dlg = ft.AlertDialog(
            content=ft.Container(
                ft.Image(src_base64=base64Img)
            )
        )
        self.page.dialog = dlg
        dlg.open = True
        self.page.update()

    def update(self):
        for strategyCard in self.strategyCards:
            strategyCard.updateStrategy()

        totalMtm = sum([strategy.getOverallPnL()
                       for strategy in self.strategies])
        self.totalMtm.value = f'₹ {totalMtm:.2f}'
        self.totalMtm.color = "green" if totalMtm >= 0 else "red"
        self.feedIcon.color = "green" if self.feed.isDataFeedAlive() else "red"
        self.feedText.value = (f'Quote       : {self.feed.getLastUpdatedDateTime()}\nReceived  : '
                               f'{self.feed.getLastReceivedDateTime()}\nBars        '
                               f': {self.feed.getNextBarsDateTime()}')

        if self.page is None:
            # Log an error message
            print("Error: `page` is None in `updateStrategy`")
            return

        self.page.update()
