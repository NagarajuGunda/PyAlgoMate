import traceback

import flet as ft
import asyncio


class OptionRow(ft.UserControl):

    def __init__(
        self,
        underlying,
        call_instrument,
        call_ltp,
        strike,
        put_ltp,
        put_instrument,
        underlying_price,
        strike_step,
        strategy,  # Add strategy parameter
    ):
        super().__init__()
        self.underlying = underlying
        self.call_instrument = call_instrument
        self.call_ltp = call_ltp
        self.strike = strike
        self.put_ltp = put_ltp
        self.put_instrument = put_instrument
        self.underlying_price = underlying_price
        self.strike_step = strike_step
        self.call_hover = False
        self.put_hover = False
        self.strategy = strategy  # Store strategy object

    def build(self):
        self.call_ltp_text = ft.Text(
            f"{self.call_ltp:.2f}" if self.call_ltp else "",
            expand=1,
            text_align="center",
            size=16,  # Increase text size
        )
        self.put_ltp_text = ft.Text(
            f"{self.put_ltp:.2f}" if self.put_ltp else "",
            expand=1,
            text_align="center",
            size=16,  # Increase text size
        )

        atm_strike = round(self.underlying_price / self.strike_step) * self.strike_step
        is_atm = self.strike == atm_strike
        is_call_itm = not is_atm and self.strike < atm_strike
        is_put_itm = not is_atm and self.strike > atm_strike

        row_bg_color = ft.colors.BLUE_100 if is_atm else ft.colors.WHITE
        call_bg_color = ft.colors.LIGHT_GREEN_100 if is_call_itm else ft.colors.WHITE
        put_bg_color = ft.colors.LIGHT_GREEN_100 if is_put_itm else ft.colors.WHITE

        self.call_buttons = self.create_trade_buttons("call")
        self.put_buttons = self.create_trade_buttons("put")

        self.call_container = ft.Container(
            content=ft.Stack(
                [
                    ft.Container(
                        content=self.call_ltp_text,
                        alignment=ft.alignment.center,
                        expand=True,
                    ),
                    self.call_buttons,
                ],
            ),
            expand=1,
            bgcolor=call_bg_color,
            padding=10,  # Add padding to increase size
            on_hover=self.on_call_hover,
        )

        self.put_container = ft.Container(
            content=ft.Stack(
                [
                    ft.Container(
                        content=self.put_ltp_text,
                        alignment=ft.alignment.center,
                        expand=True,
                    ),
                    self.put_buttons,
                ],
            ),
            expand=1,
            bgcolor=put_bg_color,
            padding=10,  # Add padding to increase size
            on_hover=self.on_put_hover,
        )

        self._main_container = ft.Container(
            content=ft.Row(
                [
                    self.call_container,
                    ft.Container(
                        content=ft.Text(
                            f"{self.strike:.2f}",
                            expand=1,
                            text_align="center",
                            weight="bold",
                            size=16,  # Increase text size
                        ),
                        expand=1,
                        bgcolor=row_bg_color,
                        alignment=ft.alignment.center,
                        padding=10,  # Add padding to increase size
                    ),
                    self.put_container,
                ],
                expand=True,
                spacing=0,
            ),
            expand=True,
            padding=0,
        )

        return self._main_container

    def create_trade_buttons(self, option_type):
        return ft.Row(
            [
                ft.Container(
                    content=ft.Text("B", color=ft.colors.BLACK, size=12, weight="bold"),
                    width=25,
                    height=25,
                    bgcolor=ft.colors.GREY_200,
                    border_radius=6,
                    alignment=ft.alignment.center,
                    on_hover=lambda e: self.on_button_hover(e, "B", option_type),
                    on_click=lambda _: self.on_button_click("Buy", option_type),
                ),
                ft.Container(
                    content=ft.Text("S", color=ft.colors.BLACK, size=12, weight="bold"),
                    width=25,
                    height=25,
                    bgcolor=ft.colors.GREY_200,
                    border_radius=6,
                    alignment=ft.alignment.center,
                    on_hover=lambda e: self.on_button_hover(e, "S", option_type),
                    on_click=lambda _: self.on_button_click("Sell", option_type),
                ),
            ],
            alignment=ft.MainAxisAlignment.CENTER,
            spacing=3,
            visible=False,
        )

    def on_call_hover(self, e):
        self.call_hover = e.data == "true"
        self.call_buttons.visible = self.call_hover
        self.call_container.update()

    def on_put_hover(self, e):
        self.put_hover = e.data == "true"
        self.put_buttons.visible = self.put_hover
        self.put_container.update()

    def on_button_hover(self, e, button_type, option_type):
        button = e.control
        if e.data == "true":
            if button_type == "B":
                button.bgcolor = ft.colors.GREEN_100
                button.content.color = ft.colors.GREEN
            else:  # "S"
                button.bgcolor = ft.colors.RED_100
                button.content.color = ft.colors.RED_500
        else:
            button.bgcolor = ft.colors.GREY_200
            button.content.color = ft.colors.BLACK
        button.update()

    def on_button_click(self, action, option_type):
        instrument = (
            self.call_instrument if option_type == "call" else self.put_instrument
        )
        self.page.dialog = OrderDialog(
            self.page, action, option_type, instrument, self.strategy
        )
        self.page.dialog.open = True
        self.page.update()

    def update_ltp(self, call_ltp, put_ltp, underlying_price):
        if not self.page:
            return

        call_updated = False
        put_updated = False
        bg_updated = False

        if self.call_ltp != call_ltp:
            self.call_ltp = call_ltp
            self.call_ltp_text.value = (
                f"{self.call_ltp:.2f}" if self.call_ltp is not None else ""
            )
            call_updated = True

        if self.put_ltp != put_ltp:
            self.put_ltp = put_ltp
            self.put_ltp_text.value = (
                f"{self.put_ltp:.2f}" if self.put_ltp is not None else ""
            )
            put_updated = True

        if self.underlying_price != underlying_price:
            self.underlying_price = underlying_price
            atm_strike = (
                round(self.underlying_price / self.strike_step) * self.strike_step
            )
            is_atm = self.strike == atm_strike
            is_call_itm = not is_atm and self.strike < atm_strike
            is_put_itm = not is_atm and self.strike > atm_strike

            row_bg_color = ft.colors.BLUE_100 if is_atm else ft.colors.WHITE
            call_bg_color = (
                ft.colors.LIGHT_GREEN_100 if is_call_itm else ft.colors.WHITE
            )
            put_bg_color = ft.colors.LIGHT_GREEN_100 if is_put_itm else ft.colors.WHITE

            if hasattr(self, "_main_container"):
                # Update strike container background
                strike_container = self._main_container.content.controls[1]
                if strike_container.bgcolor != row_bg_color:
                    strike_container.bgcolor = row_bg_color
                    strike_container.update()

            # Update call container background
            if self.call_container.bgcolor != call_bg_color:
                self.call_container.bgcolor = call_bg_color
                call_updated = True

            # Update put container background
            if self.put_container.bgcolor != put_bg_color:
                self.put_container.bgcolor = put_bg_color
                put_updated = True

            bg_updated = True

        if call_updated:
            self.call_container.update()
        if put_updated:
            self.put_container.update()
        if bg_updated:
            self._main_container.update()


class OrderDialog(ft.AlertDialog):
    def __init__(self, page, action, option_type, instrument, strategy):
        self.page = page
        self.action = action
        self.option_type = option_type
        self.instrument = instrument
        self.strategy = strategy

        self.order_type = ft.RadioGroup(
            content=ft.Column(
                [
                    ft.Radio(value="market", label="Market"),
                    ft.Radio(value="limit", label="Limit"),
                    ft.Radio(value="stop", label="Stop"),
                    ft.Radio(value="stoplimit", label="Stop Limit"),
                ]
            ),
            on_change=self.on_order_type_change,
        )

        self.quantity_input = ft.TextField(
            label="Quantity",
            value="1",
            keyboard_type=ft.KeyboardType.NUMBER,
        )

        self.limit_price_input = ft.TextField(
            label="Limit Price",
            value=str(self.strategy.getLastPrice(self.instrument)),
            keyboard_type=ft.KeyboardType.NUMBER,
            visible=False,
        )

        self.stop_price_input = ft.TextField(
            label="Stop Price",
            value=str(self.strategy.getLastPrice(self.instrument)),
            keyboard_type=ft.KeyboardType.NUMBER,
            visible=False,
        )

        super().__init__(
            title=ft.Text("Place Order"),
            content=ft.Column(
                [
                    ft.Text(f"Action: {action}"),
                    ft.Text(f"Option Type: {option_type.capitalize()}"),
                    ft.Text(f"Instrument: {instrument}"),
                    ft.Text("Order Type:"),
                    self.order_type,
                    self.quantity_input,
                    self.limit_price_input,
                    self.stop_price_input,
                    ft.ElevatedButton("Place Order", on_click=self.place_order),
                ]
            ),
        )

    def on_order_type_change(self, _):
        order_type = self.order_type.value
        self.limit_price_input.visible = order_type in ["limit", "stoplimit"]
        self.stop_price_input.visible = order_type in ["stop", "stoplimit"]
        self.page.update()

    def show_snackbar(self, message):
        self.page.snack_bar = ft.SnackBar(content=ft.Text(message))
        self.page.snack_bar.open = True
        self.page.update()

    def place_order(self, _):
        order_type = self.order_type.value
        quantity = int(self.quantity_input.value)
        limit_price = (
            float(self.limit_price_input.value)
            if self.limit_price_input.visible
            else None
        )
        stop_price = (
            float(self.stop_price_input.value)
            if self.stop_price_input.visible
            else None
        )

        order_placed = False
        order_details = f"{self.action} {quantity} {self.instrument}"

        if self.action == "Buy":
            if order_type == "market":
                self.strategy.runAsync(
                    self.strategy.enterLongAsync(self.instrument, quantity)
                )
                order_placed = True
            elif order_type == "limit":
                self.strategy.runAsync(
                    self.strategy.enterLongLimitAsync(
                        self.instrument, limit_price, quantity
                    )
                )
                order_placed = True
                order_details += f" at limit {limit_price}"
            elif order_type == "stop":
                self.strategy.runAsync(
                    self.strategy.enterLongStopAsync(
                        self.instrument, stop_price, quantity
                    )
                )
                order_placed = True
                order_details += f" at stop {stop_price}"
            elif order_type == "stoplimit":
                self.strategy.runAsync(
                    self.strategy.enterLongStopLimitAsync(
                        self.instrument, stop_price, limit_price, quantity
                    )
                )
                order_placed = True
                order_details += f" at stop {stop_price} limit {limit_price}"
        else:  # Sell
            if order_type == "market":
                self.strategy.runAsync(
                    self.strategy.enterShortAsync(self.instrument, quantity)
                )
                order_placed = True
            elif order_type == "limit":
                self.strategy.runAsync(
                    self.strategy.enterShortLimitAsync(
                        self.instrument, limit_price, quantity
                    )
                )
                order_placed = True
                order_details += f" at limit {limit_price}"
            elif order_type == "stop":
                self.strategy.runAsync(
                    self.strategy.enterShortStopAsync(
                        self.instrument, stop_price, quantity
                    )
                )
                order_placed = True
                order_details += f" at stop {stop_price}"
            elif order_type == "stoplimit":
                self.strategy.runAsync(
                    self.strategy.enterShortStopLimitAsync(
                        self.instrument, stop_price, limit_price, quantity
                    )
                )
                order_placed = True
                order_details += f" at stop {stop_price} limit {limit_price}"

        self.page.dialog.open = False
        self.page.update()

        if order_placed:
            self.show_snackbar(f"Order placed: {order_details}")
        else:
            self.show_snackbar("Failed to place order. Please try again.")


class TakeTradeView(ft.View):
    def __init__(
        self, strategy, get_positions_callback, width: float = 1000, page=None
    ):
        super().__init__(route="/take_trade")
        self.strategy = strategy
        self.get_positions_callback = get_positions_callback
        self.width = width
        self.page = page
        self.underlying = self.strategy.underlying
        self.option_chain = None
        self.__did_mount = False
        self.option_rows = {}
        self.option_contracts = {}  # New attribute to store option contracts
        self.strike_step = 100  # Assuming strike difference is 100

        self.appbar = ft.AppBar(
            title=ft.Text("Adding a Leg", size=20, weight="bold"),
            bgcolor=ft.colors.BLUE,
            color=ft.colors.WHITE,
        )

        self.content = self.create_content()
        self.controls = [self.appbar, self.content]

    def did_mount(self):
        self.__did_mount = True
        self.initialize_option_contracts()
        self.update_option_chain()

    def initialize_option_contracts(self):
        instruments = self.strategy.getFeed().getKeys()
        for instrument in instruments:
            option_contract = self.strategy.getBroker().getOptionContract(instrument)
            if option_contract and option_contract.underlying == self.underlying:
                if option_contract.strike not in self.option_contracts:
                    self.option_contracts[option_contract.strike] = {
                        "c": None,
                        "p": None,
                    }
                self.option_contracts[option_contract.strike][
                    option_contract.type
                ] = instrument

    def update_option_chain(self):
        if not self.__did_mount:
            return

        underlying_price = self.strategy.getLastPrice(self.underlying)
        atm_strike = round(underlying_price / self.strike_step) * self.strike_step

        strikes_to_display = sorted(list(self.option_contracts.keys()))
        atm_index = strikes_to_display.index(atm_strike)

        number_of_strikes_to_display = 21
        number_of_strikes_to_display_on_each_side = (
            number_of_strikes_to_display - 1
        ) // 2
        start_index = max(0, atm_index - number_of_strikes_to_display_on_each_side)
        end_index = min(
            len(strikes_to_display), start_index + number_of_strikes_to_display
        )

        # If we're at the end of the list, adjust the start_index
        if end_index == len(strikes_to_display):
            start_index = max(0, end_index - number_of_strikes_to_display)

        strikes_to_display = strikes_to_display[start_index:end_index]

        chain_updated = False
        new_controls = []

        for strike in strikes_to_display:
            call_instrument = self.option_contracts[strike]["c"]
            put_instrument = self.option_contracts[strike]["p"]
            call_ltp = (
                self.strategy.getLastPrice(call_instrument) if call_instrument else None
            )
            put_ltp = (
                self.strategy.getLastPrice(put_instrument) if put_instrument else None
            )

            if strike in self.option_rows:
                row = self.option_rows[strike]
                if (
                    row.call_ltp != call_ltp
                    or row.put_ltp != put_ltp
                    or row.underlying_price != underlying_price
                ):
                    row.update_ltp(call_ltp, put_ltp, underlying_price)
                    chain_updated = True
            else:
                row = OptionRow(
                    self.underlying,
                    call_instrument,
                    call_ltp,
                    strike,
                    put_ltp,
                    put_instrument,
                    underlying_price,
                    self.strike_step,
                    self.strategy,  # Pass strategy object here
                )
                self.option_rows[strike] = row
                chain_updated = True

            new_controls.append(row)

        # Update the option chain controls
        if (
            chain_updated or len(new_controls) != len(self.option_chain.controls) - 1
        ):  # -1 for header row
            self.option_chain.controls = [
                self.option_chain.controls[0]
            ] + new_controls  # Keep the header row
            self.option_chain.update()

        # Remove rows that are no longer in the display range
        self.option_rows = {
            strike: row
            for strike, row in self.option_rows.items()
            if strike in strikes_to_display
        }

    def create_content(self):
        return ft.Row(
            [
                ft.Column(
                    [
                        self.create_contract_selector(),
                        self.create_option_chain(),
                    ],
                    expand=5,
                ),
                ft.VerticalDivider(width=1),
                ft.Column(
                    [
                        ft.Text("Positions to be taken", size=16),
                        # Add your positions UI here
                    ],
                    expand=5,
                ),
            ],
            expand=True,
            spacing=20,
        )

    def create_contract_selector(self):
        self.contract_selector = ft.Container(
            content=self._get_contract_selector_content(),
            bgcolor=ft.colors.GREY_200,
            padding=10,
            border_radius=5,
        )
        return self.contract_selector

    def _get_contract_selector_content(self):
        underlying_price = self.strategy.getLastPrice(self.underlying)
        previous_close = (
            self.strategy.getFeed()
            .getDataSeries(self.underlying)
            .getCloseDataSeries()[-2]
        )
        change = underlying_price - previous_close
        change_percent = (change / previous_close) * 100

        return ft.Row(
            [
                ft.Text(
                    f"{self.underlying}: {underlying_price:.2f}",
                    color=ft.colors.BLUE,
                    weight="bold",
                ),
                ft.Text(
                    f"{change:.2f} ({change_percent:.2f}%)",
                    color=ft.colors.RED if change < 0 else ft.colors.GREEN,
                ),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )

    def update_contract_selector(self):
        if not self.__did_mount or not hasattr(self, "contract_selector"):
            return

        self.contract_selector.content = self._get_contract_selector_content()
        if self.contract_selector.page:
            self.contract_selector.update()

    def create_option_chain(self):
        headers = ["Call LTP", "Strike", "Put LTP"]
        header_row = ft.Container(
            content=ft.Row(
                [
                    ft.Text(h, weight="bold", expand=1, text_align="center", size=16)
                    for h in headers
                ],
                expand=1,
                spacing=0,
            ),
            bgcolor=ft.colors.GREY_200,
            padding=10,  # Add padding to match OptionRow
            border=ft.border.only(bottom=ft.border.BorderSide(1, ft.colors.GREY_400)),
        )

        self.option_chain = ft.Column(
            [header_row],
            spacing=0,
            scroll=ft.ScrollMode.ALWAYS,
            expand=True,
        )
        return self.option_chain

    def create_stop_loss_target(self):
        return ft.Container(
            content=ft.Column(
                [
                    ft.Text("Stop Loss & Target", weight="bold"),
                    self.create_input_row("Target Profit", "Stop Loss", "Trail SL"),
                    self.create_input_row("Reentry on Tgt", "Reentry on SL"),
                ],
                spacing=10,
            ),
            padding=10,
            border=ft.border.all(1, ft.colors.GREY_300),
            border_radius=5,
        )

    def create_input_row(self, *labels):
        return ft.Row(
            [
                ft.Column(
                    [
                        ft.Text(label, size=12),
                        ft.TextField(width=100, height=35),
                    ],
                    spacing=5,
                )
                for label in labels
            ],
            spacing=10,
        )

    def reload(self):
        if not self.__did_mount:
            return

    def updateData(self):
        if not self.__did_mount:
            return
        try:
            self.update_contract_selector()
            self.update_option_chain()
        except Exception as e:
            print(f"Error updating TakeTradeView: {e}")
            traceback.print_exc()
