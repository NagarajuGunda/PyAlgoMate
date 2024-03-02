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
            ft.DataColumn(ft.Text("S.NO.")),
            ft.DataColumn(ft.Text("Symbol")),
            ft.DataColumn(ft.Text("Entry Price")),
            ft.DataColumn(ft.Text("Entry Time")),
            ft.DataColumn(ft.Text("Exit Price")),
            ft.DataColumn(ft.Text("Exit Time")),
            ft.DataColumn(ft.Text("PNL")),
            ft.DataColumn(ft.Text("Action"))
        ]
        rows = []
        index = 1
        for position in self.strategy.getActivePositions().copy():
            rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(index)),
                        ft.DataCell(ft.Text(position.getInstrument())),
                        ft.DataCell(ft.Text(position.getEntryOrder().getAvgFillPrice() if position.getEntryOrder() else '')),
                        ft.DataCell(ft.Text(position.getEntryOrder().getSubmitDateTime() if position.getEntryOrder() else '')),
                        ft.DataCell(ft.Text(position.getExitOrder().getAvgFillPrice() if position.getExitOrder() else '')),
                        ft.DataCell(ft.Text(position.getExitOrder().getSubmitDateTime() if position.getExitOrder() else '')),
                        ft.DataCell(ft.Text(position.getPnL())),
                        ft.DataCell(ft.TextButton(text="Exit Position")),
                    ]))
            index = index + 1
        datatable = ft.DataTable(
            bgcolor="white",
            border=ft.border.all(2, "red"),
            border_radius=10,
            vertical_lines=ft.border.BorderSide(3, "red"),
            horizontal_lines=ft.border.BorderSide(1, "red"),
            sort_column_index=0,
            sort_ascending=True,
            heading_row_color=ft.colors.BLACK12,
            heading_row_height=100,
            data_row_color={"hovered": "0x30FF0000"},
            show_checkbox_column=True,
            divider_thickness=0,
            column_spacing=100,
            columns=columns,
            rows=rows)

        self.controls = [
            ft.ElevatedButton("Go Home", on_click=lambda _: page.go("/")),
            datatable
        ]
