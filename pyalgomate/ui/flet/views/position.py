import flet as ft
from pyalgomate.ui.flet.views.payoff import PayoffView
from pyalgomate.ui.flet.views.take_trade import TakeTradeView


class ExpandableLegRow(ft.UserControl):
    def __init__(self, position, data, colors, width):
        super().__init__()
        self.position = position
        self.data = data
        self.colors = colors
        self.width = width
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
        self.expand_icon = ft.IconButton(
            icon=ft.icons.KEYBOARD_ARROW_DOWN,
            on_click=self.toggle_expand,
            icon_size=16,
        )

        self.expanded_details = ft.Container(
            content=ft.Column(
                [
                    self.create_detail_row("ENTRY:"),
                    (
                        self.create_detail_row("EXIT:")
                        if self.position.getExitOrder() is not None
                        and self.position.getExitOrder().isFilled()
                        else ft.Container()
                    ),
                ],
                spacing=0,
            ),
            visible=False,
            bgcolor=ft.colors.GREY_100,
            padding=ft.padding.only(top=5, bottom=5),
        )

        return ft.Column(
            [
                ft.Container(
                    content=ft.Row(
                        [self.row, self.expand_icon],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    padding=ft.padding.symmetric(vertical=5),
                ),
                ft.Divider(height=1, color=ft.colors.GREY_300),
                self.expanded_details,
            ],
            spacing=0,
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

    def create_detail_row(self, label):
        order = (
            self.position.getEntryOrder()
            if label == "ENTRY:"
            else self.position.getExitOrder()
        )
        order_type = order.getType()

        details = [
            ("Type", str(order_type)),
            (
                "Limit",
                (
                    f"{order.getLimitPrice():.2f}"
                    if hasattr(order, "getLimitPrice") and order.getLimitPrice()
                    else "NA"
                ),
            ),
            (
                "Stop",
                (
                    f"{order.getStopPrice():.2f}"
                    if hasattr(order, "getStopPrice") and order.getStopPrice()
                    else "NA"
                ),
            ),
            (
                "Fill",
                f"{order.getAvgFillPrice():.2f}" if order.getAvgFillPrice() else "NA",
            ),
            (
                "Submit",
                (
                    order.getSubmitDateTime().strftime("%H:%M:%S")
                    if order.getSubmitDateTime()
                    else "NA"
                ),
            ),
            (
                "Exec",
                (
                    order.getExecutionInfo().getDateTime().strftime("%H:%M:%S")
                    if order.getExecutionInfo()
                    else "NA"
                ),
            ),
        ]

        return ft.Container(
            content=ft.Row(
                [
                    ft.Text(label, size=14, weight=ft.FontWeight.BOLD, width=60),
                    *[
                        ft.Container(
                            content=ft.Row(
                                [
                                    ft.Text(k, size=12, color=ft.colors.GREY_700),
                                    ft.Text(v, size=12, weight=ft.FontWeight.W_500),
                                ],
                                spacing=5,
                            ),
                            width=100,
                            padding=ft.padding.only(right=10),
                        )
                        for k, v in details
                    ],
                ],
                alignment=ft.MainAxisAlignment.START,
                spacing=5,
            ),
            padding=ft.padding.only(left=10, top=5, bottom=5),
            width=self.width - 40,  # Adjust width to occupy full row
        )

    def update_data(self):
        if not self.data:
            return

        mtm = self.position.getPnL()
        ltp = self.position.getLastPrice()

        # Update MTM and LTP in the data
        self.data[-1] = f"{mtm:.2f}"
        self.data[-2] = f"{ltp:.2f}"
        self.colors[-1] = ft.colors.GREEN if mtm >= 0 else ft.colors.RED

        # Recreate the row with updated data
        self.row = self.create_row()

        # Update the container's content
        self.controls[0].controls[0].content.controls[0] = self.row
        self.update()


class LegTable(ft.UserControl):
    def __init__(self, title, headers, rows):
        super().__init__()
        self.title = title
        self.headers = headers
        self.rows = rows
        self.all_expanded = False

    def build(self):
        self.expand_all_button = ft.TextButton(
            text="Expand All",
            style=ft.ButtonStyle(color=ft.colors.BLUE),
            on_click=self.toggle_all_rows,
        )

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
                        self.expand_all_button,
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                header_row,
                *self.rows,
            ],
            spacing=0,
        )

    def toggle_all_rows(self, e):
        self.all_expanded = not self.all_expanded
        for row in self.rows:
            row.expanded = self.all_expanded
            row.expand_icon.icon = (
                ft.icons.KEYBOARD_ARROW_UP
                if self.all_expanded
                else ft.icons.KEYBOARD_ARROW_DOWN
            )
            row.expanded_details.visible = self.all_expanded
        self.expand_all_button.text = (
            "Collapse All" if self.all_expanded else "Expand All"
        )
        self.update()

    def update_data(self):
        for row in self.rows:
            row.update_data()
        self.update()


class PositionView(ft.View):

    def __init__(
        self, strategy, get_positions_callback, width: float = 1000, page=None
    ):
        super().__init__(route="/positions")
        self.strategy = strategy
        self.get_positions_callback = get_positions_callback
        self.width = width
        self.expand = True
        self.__did_mount = False
        self.page = page

        self.appbar = ft.AppBar(
            title=ft.Text("Positions", size=20, weight="bold"),
            bgcolor=ft.colors.BLUE_600,
            color=ft.colors.WHITE,
            actions=[
                ft.IconButton(icon=ft.icons.CAMERA_ALT, tooltip="Screenshot"),
                ft.IconButton(icon=ft.icons.BAR_CHART, tooltip="Analyse"),
                ft.TextButton("MTM Graph", style=ft.ButtonStyle(color=ft.colors.BLUE)),
                ft.TextButton(
                    "Payoff",
                    on_click=self.show_payoff_view,
                    style=ft.ButtonStyle(color=ft.colors.BLUE),
                ),
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
                                    self.create_status_container(),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text("Open Position", weight=ft.FontWeight.BOLD),
                                    ft.Text(self.get_open_position_count()),
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
                                            ft.Text("NA"),
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
                                    ft.Text("₹ 0.00", color=ft.colors.GREEN),
                                ],
                                spacing=2,
                                alignment=ft.MainAxisAlignment.CENTER,
                            ),
                            ft.Column(
                                [
                                    ft.Text(
                                        "Margin Blocked",
                                        weight=ft.FontWeight.BOLD,
                                    ),
                                    ft.Text("NA"),
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
                    on_click=self.show_take_trade_view,
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
            "LTP",
            "MTM",
        ]
        running_legs_rows = self.prepare_running_legs_data()
        return LegTable("Running Legs", running_legs_headers, running_legs_rows)

    def create_closed_legs_table(self):
        closed_legs_headers = [
            "Instrument",
            "Qty",
            "Entry Price",
            "Entry Time",
            "Initial SL",
            "Exit Price",
            "Exit Time",
            "MTM",
        ]
        closed_legs_rows = self.prepare_closed_legs_data()
        return LegTable("Closed Legs", closed_legs_headers, closed_legs_rows)

    def prepare_running_legs_data(self):
        running_legs_rows = []
        for position in self.get_positions_callback():
            if (
                position.getEntryOrder() is not None
                and position.getEntryOrder().isFilled()
                and (
                    position.getExitOrder() is None
                    or not position.getExitOrder().isFilled()
                )
            ):
                instrument = position.getInstrument()
                qty = position.getEntryOrder().getQuantity()
                entry_price = position.getEntryOrder().getAvgFillPrice()
                entry_time = (
                    position.getEntryOrder()
                    .getExecutionInfo()
                    .getDateTime()
                    .strftime("%H:%M:%S")
                )
                ltp = position.getLastPrice()
                mtm = position.getPnL()

                row_data = [
                    instrument,
                    str(qty),
                    f"{entry_price:.2f}",
                    entry_time,
                    "N/A",  # Initial SL
                    f"{ltp:.2f}",
                    f"{mtm:.2f}",
                ]
                row_colors = [None] * 7  # Initialize with 7 None values
                row_colors[-1] = ft.colors.GREEN if mtm >= 0 else ft.colors.RED

                running_legs_rows.append(
                    ExpandableLegRow(position, row_data, row_colors, width=self.width)
                )
        return running_legs_rows

    def prepare_closed_legs_data(self):
        closed_legs_rows = []
        for position in self.get_positions_callback():
            if (
                position.getEntryOrder() is not None
                and position.getEntryOrder().isFilled()
                and position.getExitOrder() is not None
                and position.getExitOrder().isFilled()
            ):
                instrument = position.getInstrument()
                qty = position.getEntryOrder().getQuantity()
                entry_price = position.getEntryOrder().getAvgFillPrice()
                entry_time = (
                    position.getEntryOrder()
                    .getExecutionInfo()
                    .getDateTime()
                    .strftime("%H:%M:%S")
                )
                exit_price = position.getExitOrder().getAvgFillPrice()
                exit_time = (
                    position.getExitOrder()
                    .getExecutionInfo()
                    .getDateTime()
                    .strftime("%H:%M:%S")
                )
                mtm = position.getPnL()

                row_data = [
                    instrument,
                    str(qty),
                    f"{entry_price:.2f}",
                    entry_time,
                    "N/A",  # Initial SL
                    f"{exit_price:.2f}",
                    exit_time,
                    f"{mtm:.2f}",
                ]
                row_colors = [None] * 8  # Initialize with 8 None values
                row_colors[-1] = ft.colors.GREEN if mtm >= 0 else ft.colors.RED

                closed_legs_rows.append(
                    ExpandableLegRow(position, row_data, row_colors, width=self.width)
                )
        return closed_legs_rows

    def create_status_container(self):
        is_running = any(
            position.getEntryOrder().isFilled()
            and (
                position.getExitOrder() is None
                or not position.getExitOrder().isFilled()
            )
            for position in self.get_positions_callback()
        )
        status_text = "RUNNING" if is_running else "IDLE"
        status_color = ft.colors.GREEN if is_running else ft.colors.ORANGE

        return ft.Container(
            ft.Text(status_text, color=ft.colors.WHITE, size=12),
            bgcolor=status_color,
            border_radius=5,
            padding=ft.padding.symmetric(horizontal=8, vertical=2),
        )

    def get_open_position_count(self):
        return str(
            sum(
                1
                for position in self.get_positions_callback()
                if position.getEntryOrder().isFilled()
                and (
                    position.getExitOrder() is None
                    or not position.getExitOrder().isFilled()
                )
            )
        )

    def reload(self):
        if not self.__did_mount:
            return

        self.positions = self.get_positions_callback()

        # Recreate tables with new data
        self.running_legs_table = self.create_running_legs_table()
        self.closed_legs_table = self.create_closed_legs_table()

        # Update the content
        self.content.controls[2] = self.running_legs_table
        self.content.controls[3] = self.closed_legs_table

        # Update status section
        self.status_section.content.controls[0].controls[0].controls[
            1
        ] = self.create_status_container()
        self.status_section.content.controls[0].controls[1].controls[
            1
        ].value = self.get_open_position_count()

        total_mtm = sum(position.getPnL() for position in self.positions)
        mtm_column = self.status_section.content.controls[0].controls[3]
        mtm_column.controls[1].value = f"₹ {total_mtm:.2f}"
        mtm_column.controls[1].color = (
            ft.colors.GREEN if total_mtm >= 0 else ft.colors.RED
        )

        self.update()

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
        mtm_column = self.status_section.content.controls[0].controls[3]
        mtm_column.controls[1].value = f"₹ {total_mtm:.2f}"
        mtm_column.controls[1].color = (
            ft.colors.GREEN if total_mtm >= 0 else ft.colors.RED
        )

        # Update status and open position count
        status_column = self.status_section.content.controls[0].controls[0]
        status_column.controls[1] = self.create_status_container()

        open_position_column = self.status_section.content.controls[0].controls[1]
        open_position_column.controls[1].value = self.get_open_position_count()

        self.content.update()

    def show_payoff_view(self, _):
        if self.page:
            payoff_view = PayoffView(
                self.strategy, self.get_positions_callback, width=self.width
            )
            self.page.views.append(payoff_view)
            self.page.update()

    def show_take_trade_view(self, _):
        if self.page:
            take_trade_view = TakeTradeView(
                self.strategy,
                self.get_positions_callback,
                width=self.width,
                page=self.page,
            )
            self.page.views.append(take_trade_view)
            self.page.go("/take_trade")
