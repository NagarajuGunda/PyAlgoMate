from typing import List

import flet as ft
from pyalgomate.strategy.position import Position


class ExpandableLegRow(ft.UserControl):

    def __init__(self, position, data, colors, entry_details, exit_details):
        super().__init__()
        self.position = position
        self.data = data
        self.colors = colors
        self.entry_details = entry_details
        self.exit_details = exit_details
        self.expanded = False
        self.row = self.create_row()

    def create_row(self):
        return ft.Row(
            [
                ft.Text(
                    cell[0] if isinstance(cell, tuple) else cell,
                    color=color,
                    size=12,
                    width=200 if i == 0 else 70,
                    text_align="left" if i == 0 else "right",
                )
                for i, (cell, color) in enumerate(zip(self.data, self.colors))
            ],
            alignment=ft.MainAxisAlignment.START,
        )

    def build(self):
        self.copy_icon = ft.IconButton(
            icon=ft.icons.CONTENT_COPY_OUTLINED,
            icon_size=16,
            on_click=self.copy_row,
        )

        self.expand_icon = ft.IconButton(
            icon=ft.icons.KEYBOARD_ARROW_DOWN,
            on_click=self.toggle_expand,
            icon_size=16,
        )

        self.expanded_details = ft.Column(
            [
                self.create_detail_row("ENTRY:", self.entry_details),
                self.create_detail_row("EXIT:", self.exit_details),
            ],
            visible=False,
        )

        return ft.Column(
            [
                ft.Container(
                    content=ft.Row(
                        [self.row, self.copy_icon, self.expand_icon],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    border=ft.border.only(bottom=ft.BorderSide(1, ft.colors.GREY_300)),
                    padding=ft.padding.symmetric(vertical=5),
                ),
                self.expanded_details,
            ]
        )

    def create_detail_row(self, label, details):
        return ft.Container(
            content=ft.Row(
                [
                    ft.Text(label, size=12, width=50, weight=ft.FontWeight.BOLD),
                    ft.Row(
                        [
                            ft.Text(f"{k}: {v}", size=12, width=200)
                            for k, v in details.items()
                        ],
                        wrap=True,
                    ),
                ],
                alignment=ft.MainAxisAlignment.START,
            ),
            bgcolor=ft.colors.GREY_100,
            padding=5,
        )

    def toggle_expand(self, e):
        self.expanded = not self.expanded
        self.expand_icon.icon = (
            ft.icons.KEYBOARD_ARROW_UP
            if self.expanded
            else ft.icons.KEYBOARD_ARROW_DOWN
        )
        self.expanded_details.visible = self.expanded
        self.update()

    def copy_row(self, e):
        # Implement copy functionality here
        pass

    def update_data(self):
        mtm = self.position.getPnL()
        ltp = self.position.getLastPrice()

        # Update MTM and LTP in the data
        self.data[-1] = f"{mtm:.2f}"
        self.data[-2] = f"{ltp:.2f}"
        self.colors[-1] = ft.colors.GREEN if mtm >= 0 else ft.colors.RED

        # Update the row
        self.row.controls[-1].value = self.data[-1]
        self.row.controls[-1].color = self.colors[-1]
        self.row.controls[-2].value = self.data[-2]
        self.update()


class LegTable(ft.UserControl):

    def __init__(self, title, headers, rows, entry_details, exit_details):
        super().__init__()
        self.title = title
        self.headers = headers
        self.rows = rows
        self.entry_details = entry_details
        self.exit_details = exit_details

    def build(self):
        header_row = ft.Container(
            content=ft.Row(
                [
                    ft.Text(
                        h,
                        color=ft.colors.BLACK,
                        size=12,
                        weight=ft.FontWeight.BOLD,
                        width=200 if i == 0 else 70,
                        text_align="left" if i == 0 else "right",
                    )
                    for i, h in enumerate(self.headers)
                ]
                + [ft.Text("", width=80)],  # Space for icons
                alignment=ft.MainAxisAlignment.START,
            ),
            bgcolor=ft.colors.BLUE_GREY_100,
            padding=ft.padding.symmetric(vertical=8, horizontal=5),
        )

        return ft.Column(
            [
                ft.Row(
                    [
                        ft.Text(self.title, size=16, weight=ft.FontWeight.BOLD),
                        ft.TextButton(
                            "Expand All", style=ft.ButtonStyle(color=ft.colors.BLUE)
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                header_row,
                *self.rows,
            ],
            spacing=0,
        )

    def update_data(self):
        for row in self.rows:
            row.update_data()
        self.update()


class PositionView(ft.View):

    def __init__(self, positions: List[Position]):
        super().__init__(route="/positions")
        self.positions = positions
        self.expand = True
        self.__did_mount = False

        self.appbar = ft.AppBar(
            title=ft.Text("Positions", size=20, weight="bold"),
            bgcolor=ft.colors.BLUE_600,
            color=ft.colors.WHITE,
            actions=[
                ft.IconButton(icon=ft.icons.CAMERA_ALT, tooltip="Screenshot"),
                ft.IconButton(icon=ft.icons.BAR_CHART, tooltip="Analyse"),
                ft.TextButton("MTM Graph", style=ft.ButtonStyle(color=ft.colors.BLUE)),
            ],
        )

        self.status_section = ft.Container(
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Column(
                                [
                                    ft.Text("Status", weight=ft.FontWeight.BOLD),
                                    ft.Container(
                                        ft.Text(
                                            "RUNNING", color=ft.colors.WHITE, size=12
                                        ),
                                        bgcolor=ft.colors.GREEN,
                                        border_radius=5,
                                        padding=ft.padding.symmetric(
                                            horizontal=8, vertical=2
                                        ),
                                    ),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text("Open Position", weight=ft.FontWeight.BOLD),
                                    ft.Text("1"),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text("Broker", weight=ft.FontWeight.BOLD),
                                    ft.Row(
                                        [
                                            ft.Icon(
                                                ft.icons.VERIFIED_USER,
                                                color=ft.colors.BLUE,
                                                size=16,
                                            ),
                                            ft.Text("Nagaraj"),
                                            ft.Text("0"),
                                        ],
                                        spacing=2,
                                    ),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text(
                                        "Include Brokerage", weight=ft.FontWeight.BOLD
                                    ),
                                    ft.Switch(
                                        value=True,
                                        active_color=ft.colors.BLUE,
                                        inactive_thumb_color=ft.colors.BLUE_GREY,
                                        thumb_color=ft.colors.WHITE,
                                        track_color=ft.colors.BLUE_100,
                                        inactive_track_color=ft.colors.GREY_300,
                                        scale=0.8,
                                    ),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text(
                                        "Taxes & charges", weight=ft.FontWeight.BOLD
                                    ),
                                    ft.Row(
                                        [
                                            ft.Switch(
                                                value=True,
                                                active_color=ft.colors.BLUE,
                                                inactive_thumb_color=ft.colors.BLUE_GREY,
                                                thumb_color=ft.colors.WHITE,
                                                track_color=ft.colors.BLUE_100,
                                                inactive_track_color=ft.colors.GREY_300,
                                                scale=0.8,
                                            ),
                                            ft.Text("₹ 4.44", size=12),
                                            ft.Icon(
                                                ft.icons.KEYBOARD_ARROW_DOWN, size=16
                                            ),
                                        ],
                                        spacing=2,
                                    ),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text("MTM", weight=ft.FontWeight.BOLD),
                                    ft.Text("₹ 8.06", color=ft.colors.GREEN),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text(
                                        "Margin Blocked (approx)",
                                        weight=ft.FontWeight.BOLD,
                                    ),
                                    ft.Text("₹ 69,081.1"),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                ],
                spacing=10,
            ),
            padding=10,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=5,
        )

        self.action_buttons = ft.Row(
            [
                ft.ElevatedButton(
                    "+ Add Leg",
                    style=ft.ButtonStyle(color=ft.colors.WHITE, bgcolor=ft.colors.BLUE),
                ),
                ft.OutlinedButton("Switch to manual"),
                ft.OutlinedButton("Square Off"),
            ],
            alignment=ft.MainAxisAlignment.END,
        )

        self.running_legs_table = self.create_running_legs_table()
        self.closed_legs_table = self.create_closed_legs_table()

        self.deployment_time = ft.Text(
            "Deployment Time Aug 29, 2024, 08:39:25 AM", size=12, color=ft.colors.GREY
        )

        self.content = ft.ListView(
            [
                self.status_section,
                self.action_buttons,
                self.running_legs_table,
                self.closed_legs_table,
                self.deployment_time,
            ],
            spacing=15,
            padding=20,
            expand=True,
        )

        self.controls = [self.appbar, self.content]

    def did_mount(self):
        super().did_mount()

        self.__did_mount = True

    def create_running_legs_table(self):
        running_legs_headers = [
            "Instrument",
            "Qty",
            "Entry Price",
            "Entry Time",
            "Initial SL",
            "Updated SL",
            "Target",
            "Underlying",
            "LTP",
            "MTM",
        ]
        running_legs_rows = self.prepare_running_legs_data()
        return LegTable("Running Legs", running_legs_headers, running_legs_rows, {}, {})

    def create_closed_legs_table(self):
        closed_legs_headers = [
            "Instrument",
            "Qty",
            "Entry Price",
            "Entry Time",
            "Initial SL",
            "Updated SL",
            "Target",
            "Exit Price",
            "Exit Time",
            "MTM",
        ]
        closed_legs_rows = self.prepare_closed_legs_data()
        return LegTable("Closed Legs", closed_legs_headers, closed_legs_rows, {}, {})

    def prepare_running_legs_data(self):
        running_legs_rows = []
        for position in self.positions:
            if (
                position.getEntryOrder().isFilled()
                and not position.getExitOrder().isFilled()
            ):
                entry_order = position.getEntryOrder()
                instrument = position.getInstrument()
                qty = entry_order.getQuantity()
                entry_price = f"{'S' if entry_order.isSell() else 'B'} {entry_order.getAvgFillPrice():.2f}"
                entry_time = (
                    entry_order.getExecutionInfo().getDateTime().strftime("%H:%M:%S")
                )
                ltp = position.getLastPrice()
                mtm = position.getPnL()

                row_data = [
                    instrument,
                    str(qty),
                    entry_price,
                    entry_time,
                    "N/A",
                    "N/A",
                    "N/A",
                    "N/A",  # Initial SL, Updated SL, Target, Underlying
                    f"{ltp:.2f}",
                    f"{mtm:.2f}",
                ]
                row_colors = [
                    None,
                    None,
                    ft.colors.RED if entry_order.isSell() else ft.colors.GREEN,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    ft.colors.GREEN if mtm >= 0 else ft.colors.RED,
                ]
                entry_details = {
                    "Order ID": entry_order.getId(),
                    "Avg Fill Price": f"{entry_order.getAvgFillPrice():.2f}",
                    "Filled Qty": str(entry_order.getFilled()),
                    "Remaining Qty": str(entry_order.getRemaining()),
                    "Status": entry_order.getState(),
                }
                exit_details = {}  # Empty for running legs
                running_legs_rows.append(
                    ExpandableLegRow(
                        position, row_data, row_colors, entry_details, exit_details
                    )
                )
        return running_legs_rows

    def prepare_closed_legs_data(self):
        closed_legs_rows = []
        for position in self.positions:
            if (
                position.getEntryOrder().isFilled()
                and position.getExitOrder().isFilled()
            ):
                entry_order = position.getEntryOrder()
                exit_order = position.getExitOrder()
                instrument = position.getInstrument()
                qty = entry_order.getQuantity()
                entry_price = f"{entry_order.getAvgFillPrice():.2f} {'S' if entry_order.isSell() else 'B'}"
                entry_time = (
                    entry_order.getExecutionInfo().getDateTime().strftime("%H:%M:%S")
                )
                exit_price = f"{exit_order.getAvgFillPrice():.2f} {'B' if entry_order.isSell() else 'S'}"
                exit_time = (
                    exit_order.getExecutionInfo().getDateTime().strftime("%H:%M:%S")
                )
                mtm = position.getPnL()

                row_data = [
                    instrument,
                    str(qty),
                    entry_price,
                    entry_time,
                    "N/A",
                    "N/A",
                    "N/A",  # Initial SL, Updated SL, Target
                    exit_price,
                    exit_time,
                    f"{mtm:.2f}",
                ]
                row_colors = [
                    None,
                    None,
                    ft.colors.RED if entry_order.isSell() else ft.colors.GREEN,
                    None,
                    None,
                    None,
                    None,
                    ft.colors.GREEN if entry_order.isSell() else ft.colors.RED,
                    None,
                    ft.colors.GREEN if mtm >= 0 else ft.colors.RED,
                ]
                entry_details = {
                    "Order ID": entry_order.getId(),
                    "Avg Fill Price": f"{entry_order.getAvgFillPrice():.2f}",
                    "Filled Qty": str(entry_order.getFilled()),
                    "Status": entry_order.getState(),
                }
                exit_details = {
                    "Order ID": exit_order.getId(),
                    "Avg Fill Price": f"{exit_order.getAvgFillPrice():.2f}",
                    "Filled Qty": str(exit_order.getFilled()),
                    "Status": exit_order.getState(),
                }
                closed_legs_rows.append(
                    ExpandableLegRow(
                        position, row_data, row_colors, entry_details, exit_details
                    )
                )
        return closed_legs_rows

    def updateData(self):
        if not self.__did_mount:
            return

        total_mtm = 0

        # Update running legs
        for row in self.running_legs_table.rows:
            position = row.position
            mtm = position.getPnL()
            ltp = position.getLastPrice()

            row.data[-1] = f"{mtm:.2f}"
            row.data[-2] = f"{ltp:.2f}"
            row.colors[-1] = ft.colors.GREEN if mtm >= 0 else ft.colors.RED

            total_mtm += mtm

            row.update_data()

        # Add closed legs MTM (as it doesn't change)
        for row in self.closed_legs_table.rows:
            total_mtm += float(row.data[-1])

        # Update overall MTM in status row
        mtm_column = self.status_section.content.controls[0].controls[5]
        mtm_column.controls[1].value = f"₹ {total_mtm:.2f}"
        mtm_column.controls[1].color = (
            ft.colors.GREEN if total_mtm >= 0 else ft.colors.RED
        )

        self.content.update()
