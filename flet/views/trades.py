import flet as ft

from pyalgotrade.strategy.position import Position
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy

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

        self.collectedPremiumText = ft.Text("₹ 0", color=ft.colors.GREEN, size=25)
        self.currentPremiumText = ft.Text("₹ 0", color=ft.colors.GREEN, size=25)
        self.mtmText = ft.Text("₹ 0", color=ft.colors.GREEN, size=25)
        self.premiumsCard = ft.Card(
            ft.Container(
                ft.Row(
                    [
                        ft.Column(
                            [
                                    ft.Container(
                                        content=ft.Text("Collected Premium", color=ft.colors.BLACK38, size=15)
                                    ),
                                    ft.Container(
                                        content=self.collectedPremiumText
                                    ),
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            expand=True,
                        ),
                        ft.VerticalDivider(color=ft.colors.BLACK38, thickness=2, opacity=0.5),
                        ft.Column(
                            [
                                    ft.Container(
                                        content=ft.Text("Current Premium", color=ft.colors.BLACK38, size=15)
                                    ),
                                    ft.Container(
                                        content=self.currentPremiumText
                                    ),
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            expand=True,
                        ),
                        ft.VerticalDivider(color=ft.colors.BLACK38, thickness=2, opacity=0.5),
                        ft.Column(
                            [
                                    ft.Container(
                                        content=ft.Text("MTM", color=ft.colors.BLACK38, size=15)
                                    ),
                                    ft.Container(
                                        content=self.mtmText
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

        columns=[
            ft.DataColumn(ft.Text("Instrument")),
            ft.DataColumn(ft.Text("B/S")),
            ft.DataColumn(ft.Text("Entry Time"), on_sort=lambda e: print(f"{e.column_index}, {e.ascending}")),
            ft.DataColumn(ft.Text("Entry Price"), numeric=True),
            ft.DataColumn(ft.Text("Entry Quantity"), numeric=True),
            ft.DataColumn(ft.Text("Exit Time")),
            ft.DataColumn(ft.Text("Exit Price"), numeric=True),
            ft.DataColumn(ft.Text("Exit Quantity"), numeric=True),
            ft.DataColumn(ft.Text("PNL"), numeric=True, on_sort=lambda e: print(f"{e.column_index}, {e.ascending}")),
            ft.DataColumn(ft.Text("Close"))
        ]

        self.datatable = ft.DataTable(
            sort_column_index=2,
            sort_ascending=True,
            columns=columns,
            rows=self.getRows())

        self.controls = [
            ft.ElevatedButton("Go Home", on_click=lambda _: page.go("/")),
            self.premiumsCard,
            self.datatable
        ]

    def getRows(self):
        rows = []
        for pos in self.strategy.getActivePositions().copy().union(self.strategy.getClosedPositions().copy()):
            position: Position = pos
            icon = ft.icons.ARROW_CIRCLE_UP_SHARP if position.getEntryOrder().isBuy() else  ft.icons.ARROW_CIRCLE_DOWN_SHARP
            entryPrice = round(position.getEntryOrder().getAvgFillPrice(), 2) if position.entryFilled() else None
            entryQuantity = position.getEntryOrder().getQuantity() if position.entryFilled() else None
            exitPrice = round(position.getExitOrder().getAvgFillPrice(), 2) if position.exitFilled() else None
            exitQuantity = position.getExitOrder().getQuantity() if position.exitFilled() else None
            pnl = position.getPnL()
            pnlText = ft.Text(f'{pnl:.2f}', color="green" if pnl >= 0 else "red")
            rows.append(
                ft.DataRow(
                    [
                        ft.DataCell(ft.Text(position.getInstrument())),
                        ft.DataCell(ft.Icon(name=icon, color='green' if position.getEntryOrder().isBuy() else 'red')),
                        ft.DataCell(ft.Text(position.getEntryOrder().getSubmitDateTime() if position.getEntryOrder() else '')),
                        ft.DataCell(ft.Text(entryPrice)),
                        ft.DataCell(ft.Text(entryQuantity)),
                        ft.DataCell(ft.Text(position.getExitOrder().getSubmitDateTime() if position.getExitOrder() else '')),
                        ft.DataCell(ft.Text(exitPrice)),
                        ft.DataCell(ft.Text(exitQuantity)),
                        ft.DataCell(pnlText),
                        ft.DataCell(ft.Icon(name=ft.icons.CLOSE_SHARP, color="red400") if position.exitActive() else ft.Text('')),
                    ]
                )
            )
        return rows

    def update(self):
        self.datatable.rows = self.getRows()
        self.datatable.update()

        openPositions = self.strategy.getActivePositions().copy()
        closedPositions = self.strategy.getClosedPositions().copy()

        collectedPremium = sum(
                [
                    position.getEntryOrder().getAvgFillPrice() * position.getEntryOrder().getQuantity()
                    for position in openPositions.union(closedPositions)
                    if position.entryFilled() and position.getEntryOrder().isSell()
                ]
            )
        self.collectedPremiumText.value = f'₹ {collectedPremium:.2f}'
        self.collectedPremiumText.color = "green" if collectedPremium >= 0 else "red"
        self.collectedPremiumText.update()

        currentPremium = sum(
            [
                position.getLastPrice() * position.getEntryOrder().getQuantity()
                for position in openPositions
                if position.entryFilled() and position.getEntryOrder().isSell()
            ]
        )
        self.currentPremiumText.value = f'₹ {currentPremium:.2f}'
        self.currentPremiumText.color = "green" if currentPremium >= 0 else "red"
        self.currentPremiumText.update()

        mtm = sum(
            [
                position.getPnL() for position in openPositions.union(closedPositions)
            ]
        )
        self.mtmText.value = f'₹ {mtm:.2f}'
        self.mtmText.color = "green" if mtm >= 0 else "red"
        self.mtmText.update()
