import flet as ft

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
            ft.DataColumn(ft.Text("Entry Price"), numeric=True),
            ft.DataColumn(ft.Text("Entry Time"), on_sort=lambda e: print(f"{e.column_index}, {e.ascending}")),
            ft.DataColumn(ft.Text("Exit Price"), numeric=True),
            ft.DataColumn(ft.Text("Exit Time")),
            ft.DataColumn(ft.Text("PNL"), numeric=True),
            ft.DataColumn(ft.Text("Exit"))
        ]
        rows = []
        for position in self.strategy.getActivePositions().copy():
            rows.append(
                ft.DataRow(
                    [
                        ft.DataCell(ft.Text(position.getInstrument())),
                        ft.DataCell(ft.Text(position.getEntryOrder().getAvgFillPrice() if position.getEntryOrder() else '')),
                        ft.DataCell(ft.Text(position.getEntryOrder().getSubmitDateTime() if position.getEntryOrder() else '')),
                        ft.DataCell(ft.Text(position.getExitOrder().getAvgFillPrice() if position.getExitOrder() else '')),
                        ft.DataCell(ft.Text(position.getExitOrder().getSubmitDateTime() if position.getExitOrder() else '')),
                        ft.DataCell(ft.Text(position.getPnL())),
                        ft.DataCell(ft.TextButton(icon="close_rounded", icon_color="red400")),
                    ]
                )
            )
        datatable = ft.DataTable(
            sort_column_index=2,
            sort_ascending=True,
            columns=columns,
            rows=rows)

        self.controls = [
            ft.ElevatedButton("Go Home", on_click=lambda _: page.go("/")),
            datatable
        ]
