import pandas as pd
from typing import List
import flet as ft
from .paginated_dt import PaginatedDataTable
from pyalgotrade.strategy.position import Position
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State


class TradesView(ft.View):
    def __init__(self, page: ft.Page, strategy: BaseOptionsGreeksStrategy):
        super().__init__(route="/trades")
        self.scroll = ft.ScrollMode.HIDDEN
        self.page = page
        self.strategy = strategy

        self.appbar = ft.AppBar(
            title=ft.Text("Trades"),
            bgcolor=ft.colors.SURFACE_VARIANT
        )

        self.collectedPremiumText = ft.Text(
            "₹ 0", color=ft.colors.GREEN, size=25)
        self.currentPremiumText = ft.Text(
            "₹ 0", color=ft.colors.GREEN, size=25)
        self.mtmText = ft.Text("₹ 0", color=ft.colors.GREEN, size=25)
        self.unrealizedMtmText = ft.Text("₹ 0", color=ft.colors.GREEN, size=25)

        self.strategyViewButton = ft.ElevatedButton(
            text="Strategy View",
            on_click=self.switchToStrategyView
        )

        self.totalDeltaText = ft.Text("Δ 0", color=ft.colors.BLACK38, size=15)
        self.totalGammaText = ft.Text("Γ 0", color=ft.colors.BLACK38, size=15)
        self.totalThetaText = ft.Text("Θ 0", color=ft.colors.BLACK38, size=15)
        self.totalVegaText = ft.Text("V 0", color=ft.colors.BLACK38, size=15)
        self.totalIVText = ft.Text("IV 0", color=ft.colors.BLACK38, size=15)

        self.premiumsCard = ft.Card(
            ft.Container(
                ft.Row(
                    [
                        ft.Column(
                            [
                                ft.Container(
                                    content=ft.Text(
                                        "Collected Premium", color=ft.colors.BLACK38, size=15)
                                ),
                                ft.Container(
                                    content=self.collectedPremiumText
                                ),
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            expand=True,
                        ),
                        ft.VerticalDivider(
                            color=ft.colors.BLACK38, thickness=2, opacity=0.5),
                        ft.Column(
                            [
                                ft.Container(
                                    content=ft.Text(
                                        "Current Premium", color=ft.colors.BLACK38, size=15)
                                ),
                                ft.Container(
                                    content=self.currentPremiumText
                                ),
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            expand=True,
                        ),
                        ft.VerticalDivider(
                            color=ft.colors.BLACK38, thickness=2, opacity=0.5),
                        ft.Column(
                            [
                                ft.Container(
                                    content=ft.Text(
                                        "MTM", color=ft.colors.BLACK38, size=15)
                                ),
                                ft.Container(
                                    content=self.mtmText
                                ),
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            expand=True,
                        ),
                        ft.VerticalDivider(
                            color=ft.colors.BLACK38, thickness=2, opacity=0.5),
                        ft.Column(
                            [
                                ft.Container(
                                    content=ft.Text(
                                        "Unrealized MTM", color=ft.colors.BLACK38, size=15)
                                ),
                                ft.Container(
                                    content=self.unrealizedMtmText
                                ),
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            expand=True,
                        ),
                    ],
                    spacing=0,
                    expand=True,
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                padding=ft.padding.all(20),
                expand=True
            ),
            height=120
        )

        self.greeksRow = ft.Row(
            [
                ft.Column(
                    [
                        ft.Container(
                            content=self.totalDeltaText
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    expand=True,
                ),
                ft.Column(
                    [
                        ft.Container(
                            content=self.totalGammaText
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    expand=True,
                ),
                ft.Column(
                    [
                        ft.Container(
                            content=self.totalThetaText
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    expand=True,
                ),
                ft.Column(
                    [
                        ft.Container(
                            content=self.totalVegaText
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    expand=True,
                ),
                ft.Column(
                    [
                        ft.Container(
                            content=self.totalIVText
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    expand=True,
                ),
            ],
            spacing=0,
            expand=True,
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )

        columns = [
            ft.DataColumn(ft.Text("Instrument")),
            ft.DataColumn(ft.Text("B/S")),
            ft.DataColumn(ft.Text("Entry Time"), on_sort=lambda e: print(
                f"{e.column_index}, {e.ascending}")),
            ft.DataColumn(ft.Text("Entry Price"), numeric=True),
            ft.DataColumn(ft.Text("Entry Quantity"), numeric=True),
            ft.DataColumn(ft.Text('LTP'), numeric=True),
            ft.DataColumn(ft.Text("Exit Time")),
            ft.DataColumn(ft.Text("Exit Price"), numeric=True),
            ft.DataColumn(ft.Text("Exit Quantity"), numeric=True),
            ft.DataColumn(ft.Text("PNL"), numeric=True, on_sort=lambda e: print(
                f"{e.column_index}, {e.ascending}")),
            ft.DataColumn(ft.Text("Close"))
        ]

        self.datatable = ft.DataTable(
            sort_column_index=2,
            sort_ascending=True,
            columns=columns,
            rows=self.getRows()
        )
        self.paginatedDataTable = PaginatedDataTable(self.datatable)
        self.controls = [
            ft.Container(
                self.strategyViewButton
            ),
            self.premiumsCard,
            ft.Container(
                self.greeksRow,
                alignment=ft.alignment.center,
                padding=ft.padding.only(bottom=20)
            ),
            ft.Container(
                self.paginatedDataTable,
                alignment=ft.alignment.center
            )
        ]

    def switchToStrategyView(self, e):
        strategyView = self.strategy.getView(self.page)
        if strategyView:
            self.page.go(
                f'/strategy?strategyName={self.strategy.strategyName}')
        else:
            self.page.snack_bar = ft.SnackBar(
                ft.Row([ft.Text(f"Strategy view is not implemented !!!", size=20)],
                       alignment='center'),
                bgcolor='#263F6A'
            )
            self.page.snack_bar.open = True

    def getRows(self):
        positions: List[Position] = [
            position for position in sorted(
                self.strategy.getActivePositions().copy().union(
                    self.strategy.getClosedPositions().copy()),
                key=lambda
                position: position.getEntryOrder().getSubmitDateTime(
                ) if position.entryFilled() else pd.Timestamp.min
            )
        ]
        rows = []
        for position in positions:
            icon = ft.icons.ARROW_CIRCLE_UP_SHARP if position.getEntryOrder(
            ).isBuy() else ft.icons.ARROW_CIRCLE_DOWN_SHARP
            entryPrice = round(position.getEntryOrder(
            ).getAvgFillPrice(), 2) if position.entryFilled() else None
            entryQuantity = position.getEntryOrder(
            ).getQuantity() if position.entryFilled() else None
            exitPrice = round(position.getExitOrder(
            ).getAvgFillPrice(), 2) if position.exitFilled() else None
            exitQuantity = position.getExitOrder().getQuantity() if position.exitFilled() else None
            pnl = position.getPnL()
            pnlText = ft.Text(
                f'{pnl:.2f}', color="green" if pnl >= 0 else "red")
            rows.append(
                ft.DataRow(
                    [
                        ft.DataCell(ft.Text(position.getInstrument())),
                        ft.DataCell(ft.Icon(
                            name=icon, color='green' if position.getEntryOrder().isBuy() else 'red')),
                        ft.DataCell(ft.Text(
                            position.getEntryOrder().getExecutionInfo().getDateTime() if position.entryFilled() else '')),
                        ft.DataCell(ft.Text(entryPrice)),
                        ft.DataCell(ft.Text(entryQuantity)),
                        ft.DataCell(ft.Text(position.getLastPrice())),
                        ft.DataCell(ft.Text(
                            position.getExitOrder().getExecutionInfo().getDateTime() if position.exitFilled() else '')),
                        ft.DataCell(ft.Text(exitPrice)),
                        ft.DataCell(ft.Text(exitQuantity)),
                        ft.DataCell(pnlText),
                        ft.DataCell(ft.IconButton(icon=ft.icons.CLOSE_SHARP,
                                                  icon_color="red400",
                                                  on_click=lambda e, pos=position: self.exitWithMarketProtection(
                                                      pos)
                                                  ) if not position.exitFilled() else ft.Text('')),
                    ]
                )
            )
        return rows

    def exitWithMarketProtection(self, position):
        self.strategy.state = State.PLACING_ORDERS
        self.strategy.exitPosition(position, 15, 0.05)

    def update(self):
        openPositions = self.strategy.getActivePositions().copy()
        closedPositions = self.strategy.getClosedPositions().copy()

        collectedPremium = sum(
            [
                position.getEntryOrder().getAvgFillPrice() * position.getEntryOrder().getQuantity()
                for position in openPositions
                if position.entryFilled() and position.getEntryOrder().isSell()
            ]
        )
        collectedPremium += sum(
            [
                -position.getEntryOrder().getAvgFillPrice() *
                position.getEntryOrder().getQuantity()
                for position in openPositions
                if position.entryFilled() and position.getEntryOrder().isBuy()
            ]
        )
        self.collectedPremiumText.value = f'₹ {collectedPremium:.2f}'
        self.collectedPremiumText.color = "green" if collectedPremium >= 0 else "red"

        currentPremium = sum(
            [
                position.getLastPrice() * position.getEntryOrder().getQuantity()
                for position in openPositions
                if position.entryFilled() and position.getEntryOrder().isSell()
            ]
        )
        self.currentPremiumText.value = f'₹ {currentPremium:.2f}'
        self.currentPremiumText.color = "green" if currentPremium >= 0 else "red"

        mtm = sum(
            [
                position.getPnL() for position in openPositions.union(closedPositions)
            ]
        )
        self.mtmText.value = f'₹ {mtm:.2f}'
        self.mtmText.color = "green" if mtm >= 0 else "red"

        unrealizedMtm = sum(
            [
                position.getPnL() for position in openPositions
            ]
        )
        self.unrealizedMtmText.value = f'₹ {unrealizedMtm:.2f}'
        self.unrealizedMtmText.color = "green" if unrealizedMtm >= 0 else "red"

        self.datatable.rows = self.getRows()
        self.paginatedDataTable.refresh_data()

        greeksData = self.strategy.getGreeks(
            [position.getInstrument() for position in openPositions.union(closedPositions)])

        totalDelta = 0.0
        totalGamma = 0.0
        totalTheta = 0.0
        totalVega = 0.0
        totalIV = 0.0

        for position in openPositions:
            instrument = position.getInstrument()
            if instrument in greeksData:
                greek = greeksData[instrument]
                quantity = position.getEntryOrder().getQuantity()
                sign = 1 if position.getEntryOrder().isBuy() else -1
                totalDelta += greek.delta * quantity * sign
                totalGamma += greek.gamma * quantity * sign
                totalTheta += greek.theta * quantity * sign
                totalVega += greek.vega * quantity * sign
                totalIV += greek.iv * quantity * sign

        self.totalDeltaText.value = f'Δ {totalDelta:.2f}'
        self.totalGammaText.value = f'Γ {totalGamma:.2f}'
        self.totalThetaText.value = f'Θ {totalTheta:.2f}'
        self.totalVegaText.value = f'V {totalVega:.2f}'
        self.totalIVText.value = f'IV {totalIV:.2f}'

        self.page.update()
