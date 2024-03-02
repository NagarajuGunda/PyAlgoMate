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
        super().update()
