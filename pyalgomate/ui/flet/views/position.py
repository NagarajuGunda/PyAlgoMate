import flet as ft
from pyalgomate.core.broker import Order
from pyalgomate.ui.flet.views.payoff import PayoffView
from pyalgomate.ui.flet.views.take_trade import TakeTradeView


class ExpandableLegRow(ft.UserControl):

    def __init__(self, position, data, colors, width, on_exit=None):
        super().__init__()
        self.position = position
        self.data = data
        self.colors = colors
        self.width = width
        self.expanded = False
        self.on_exit = on_exit
        self.row = None
        self.exit_button = None
        self.create_row()

    def create_row(self):
        row_content = [
            ft.Text(
                cell[0] if isinstance(cell, tuple) else cell,
                color=color,
                size=12,
                width=200 if i == 0 else 70,
                text_align="left" if i == 0 else "right",
            )
            for i, (cell, color) in enumerate(zip(self.data, self.colors))
        ]

        if self.on_exit:
            self.exit_button = ft.ElevatedButton(
                "Exit",
                on_click=self.handle_exit_click,
                style=ft.ButtonStyle(
                    shape=ft.RoundedRectangleBorder(radius=5),
                    padding=ft.padding.all(5),
                ),
                height=25,
            )
            row_content.append(self.exit_button)

        self.row = ft.Row(
            row_content,
            alignment=ft.MainAxisAlignment.START,
        )

    def handle_exit_click(self, _):
        if self.on_exit:
            self.on_exit(self.position)

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

        # Convert order type to text
        order_type_text = self.get_order_type_text(order_type)

        details = [
            ("Type", order_type_text),
            (
                "Stop",
                (
                    f"{order.getStopPrice():.2f}"
                    if hasattr(order, "getStopPrice") and order.getStopPrice()
                    else "NA"
                ),
            ),
            (
                "Limit",
                (
                    f"{order.getLimitPrice():.2f}"
                    if hasattr(order, "getLimitPrice") and order.getLimitPrice()
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

    def get_order_type_text(self, order_type):
        order_type_map = {
            Order.Type.MARKET: "Market",
            Order.Type.LIMIT: "Limit",
            Order.Type.STOP: "Stop",
            Order.Type.STOP_LIMIT: "Stop Limit",
        }
        return order_type_map.get(order_type, "Unknown")

    def update_data(self):
        if not self.data:
            return

        mtm = self.position.getPnL()
        ltp = self.position.getLastPrice()

        # Update MTM and LTP in the data
        self.data[-1] = f"{mtm:.2f}"
        self.data[-2] = f"{ltp:.2f}"
        self.colors[-1] = ft.colors.GREEN if mtm >= 0 else ft.colors.RED

        # Update the text controls in the row
        for i, (cell, color) in enumerate(zip(self.data, self.colors)):
            self.row.controls[i].value = cell[0] if isinstance(cell, tuple) else cell
            self.row.controls[i].color = color

        # Update the exit button if it exists
        if self.exit_button:
            self.exit_button.disabled = self.position.exitFilled()

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
                    "+ Manual Trade",
                    style=ft.ButtonStyle(color=ft.colors.WHITE, bgcolor=ft.colors.BLUE),
                    on_click=self.show_take_trade_view,
                ),
                ft.OutlinedButton("Square Off", on_click=self.square_off_all_positions),
            ],
            alignment=ft.MainAxisAlignment.END,
        )

        self.running_legs_table = self.create_running_legs_table()
        self.closed_legs_table = self.create_closed_legs_table()
        self.inactive_legs_table = self.create_inactive_legs_table()

        self.deployment_time = ft.Text(
            "Deployment Time Aug 29, 2024, 08:39:25 AM", size=12, color=ft.colors.GREY
        )

        self.content = ft.ListView(
            [
                self.status_section,
                self.action_buttons,
                self.running_legs_table,
                self.closed_legs_table,
                self.inactive_legs_table,
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
            "",  # For the Exit button
        ]
        running_legs_rows = self.prepare_running_legs_data()
        return LegTable("Running Legs", running_legs_headers, running_legs_rows)

    def create_inactive_legs_table(self):
        inactive_legs_headers = [
            "Instrument",
            "Qty",
            "Entry Price",
            "Entry Time",
            "Initial SL",
            "LTP",
            "MTM",
        ]
        inactive_legs_rows = self.prepare_inactive_legs_data()
        return LegTable("Inactive Legs", inactive_legs_headers, inactive_legs_rows)

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
                    ExpandableLegRow(
                        position,
                        row_data,
                        row_colors,
                        width=self.width,
                        on_exit=self.exit_position,
                    )
                )
        return running_legs_rows

    def prepare_inactive_legs_data(self):
        inactive_legs_rows = []
        for position in self.get_positions_callback():
            if (
                position not in self.strategy.getClosedPositions()
                and not position.entryActive()
                and not position.exitActive()
            ):
                instrument = position.getInstrument()
                qty = (
                    position.getEntryOrder().getQuantity()
                    if position.getEntryOrder()
                    else 0
                )
                entry_price = (
                    position.getEntryOrder().getAvgFillPrice()
                    if position.getEntryOrder()
                    else 0
                )
                entry_time = (
                    position.getEntryOrder()
                    .getExecutionInfo()
                    .getDateTime()
                    .strftime("%H:%M:%S")
                    if position.getEntryOrder()
                    and position.getEntryOrder().getExecutionInfo()
                    else "N/A"
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
                row_colors = [None] * 7
                row_colors[-1] = ft.colors.GREEN if mtm >= 0 else ft.colors.RED

                inactive_legs_rows.append(
                    ExpandableLegRow(position, row_data, row_colors, width=self.width)
                )
        return inactive_legs_rows

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

    def exit_position(self, position):
        if self.strategy and position.exitActive():
            self.strategy.exitPosition(position, 10, 0.05)

            # Prepare position information for the snack bar
            instrument = position.getInstrument()
            quantity = position.getEntryOrder().getQuantity()
            entry_price = position.getEntryOrder().getAvgFillPrice()
            current_pnl = position.getPnL()

            # Create and show the snack bar
            snack_bar_message = (
                f"Exiting position:\n"
                f"Instrument: {instrument}\n"
                f"Quantity: {quantity}\n"
                f"Entry Price: {entry_price:.2f}\n"
                f"Current PnL: {current_pnl:.2f}"
            )

            self.page.snack_bar = ft.SnackBar(
                content=ft.Text(snack_bar_message),
                action="Dismiss",
            )
            self.page.snack_bar.open = True
            self.page.update()

    def reload(self):
        if not self.__did_mount:
            return

        self.positions = self.get_positions_callback()

        # Recreate tables with new data
        self.running_legs_table = self.create_running_legs_table()
        self.closed_legs_table = self.create_closed_legs_table()
        self.inactive_legs_table = self.create_inactive_legs_table()

        # Update the content
        self.content.controls[2] = self.running_legs_table
        self.content.controls[3] = self.closed_legs_table
        self.content.controls[4] = self.inactive_legs_table

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

        # Update inactive legs
        for row in self.inactive_legs_table.rows:
            position = row.position
            mtm = position.getPnL()
            ltp = position.getLastPrice()

            row.data[-2] = f"{ltp:.2f}"
            row.data[-1] = f"{mtm:.2f}"
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

    def square_off_all_positions(self, _):
        if self.strategy:
            self.strategy.closeAllPositions()

            message = "Squaring off all positions..."
            self.page.snack_bar = ft.SnackBar(
                content=ft.Text(message),
                action="Dismiss",
            )
            self.page.snack_bar.open = True
            self.page.update()
