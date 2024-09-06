import base64
from io import BytesIO

import matplotlib.pyplot as plt
import numpy as np

import flet as ft
from pyalgomate.strategies import OptionContract
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy


class PayoffView(ft.View):
    def __init__(
        self,
        strategy: BaseOptionsGreeksStrategy,
        get_positions_callback,
        width: float = 1000,
    ):
        super().__init__(route="/payoff")
        self.strategy = strategy
        self.get_positions_callback = get_positions_callback
        self.width = width
        self.expand = True
        self.__did_mount = False

        self.appbar = ft.AppBar(
            title=ft.Text("Payoff Analysis", size=20, weight="bold"),
            bgcolor=ft.colors.BLUE_600,
            color=ft.colors.WHITE,
        )

        self.payoff_chart = ft.Image(src=None, width=self.width, height=400)
        self.metrics = self.create_metrics_section()

        self.content = ft.Column(
            [
                self.payoff_chart,
                self.metrics,
            ],
            spacing=20,
            scroll=ft.ScrollMode.AUTO,
        )

        self.controls = [self.appbar, self.content]

    def did_mount(self):
        super().did_mount()

        self.__did_mount = True

    def create_metrics_section(self):
        return ft.Container(
            content=ft.Column(
                [
                    ft.Row(
                        [
                            self.create_metric("Total MTM", "₹ 0", ft.colors.BLACK),
                            self.create_metric("Max Profit", "₹ 0", ft.colors.GREEN),
                            self.create_metric("Max Loss", "₹ 0", ft.colors.RED),
                            self.create_metric("Margin Approx", "₹ 0", ft.colors.BLACK),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    ft.Row(
                        [
                            self.create_metric("POP", "0%", ft.colors.BLACK),
                            self.create_metric("Risk/Reward", "0", ft.colors.BLACK),
                            self.create_metric("Breakeven", "0", ft.colors.BLACK),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    ft.Row(
                        [
                            self.create_metric("Delta", "0", ft.colors.BLACK),
                            self.create_metric("Gamma", "0", ft.colors.BLACK),
                            self.create_metric("Theta", "₹ 0", ft.colors.BLACK),
                            self.create_metric("Vega", "₹ 0", ft.colors.BLACK),
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

    def create_metric(self, label, value, color):
        return ft.Column(
            [
                ft.Text(label, size=14, color=ft.colors.GREY_700),
                ft.Text(value, size=16, color=color, weight=ft.FontWeight.BOLD),
            ],
            spacing=2,
            alignment=ft.MainAxisAlignment.CENTER,
        )

    def calculate_payoff(self, positions, spot_range):
        payoff = np.zeros_like(spot_range)
        for position in positions:
            instrument = position.getInstrument()
            option_contract: OptionContract = (
                self.strategy.getBroker().getOptionContract(instrument)
            )
            if not option_contract:
                continue

            # Check if the position is closed
            if position.exitFilled():
                # For closed positions, just add the realized PnL
                payoff += position.getPnL()
            else:
                # For open positions, calculate the payoff as before
                quantity = position.getEntryOrder().getQuantity()
                entry_price = position.getEntryOrder().getAvgFillPrice()
                is_sell = position.getEntryOrder().isSell()
                sign = -1 if is_sell else 1

                if option_contract.type == "c":
                    position_payoff = (
                        np.maximum(spot_range - option_contract.strike, 0) * quantity
                        - entry_price * quantity
                    )
                elif option_contract.type == "p":
                    position_payoff = (
                        np.maximum(option_contract.strike - spot_range, 0) * quantity
                        - entry_price * quantity
                    )
                else:
                    # For futures or other instruments
                    position_payoff = (spot_range - entry_price) * quantity

                payoff += sign * position_payoff

        return payoff

    def update_payoff_chart(self):
        positions = self.get_positions_callback()
        if not positions:
            return

        spot_price = self.strategy.getLastPrice(self.strategy.underlying)
        if spot_price is None:
            return

        spot_range = np.linspace(spot_price * 0.8, spot_price * 1.2, 200)
        payoff = self.calculate_payoff(positions, spot_range)

        plt.figure(figsize=(10, 6))

        # Plot positive payoff area in green
        plt.fill_between(
            spot_range, payoff, 0, where=(payoff > 0), facecolor="green", alpha=0.3
        )

        # Plot negative payoff area in red
        plt.fill_between(
            spot_range, payoff, 0, where=(payoff <= 0), facecolor="red", alpha=0.3
        )

        # Plot the payoff line
        plt.plot(spot_range, payoff, label="Payoff", color="blue")

        # Add horizontal line at y=0
        plt.axhline(y=0, color="gray", linestyle="--")

        # Add vertical line and annotation for current spot price
        plt.axvline(x=spot_price, color="orange", linestyle="--", label="Current Spot")
        plt.annotate(
            f"Spot: {spot_price:.2f}",
            xy=(spot_price, plt.ylim()[0]),
            xytext=(5, 10),
            textcoords="offset points",
            ha="left",
            va="bottom",
            bbox=dict(boxstyle="round,pad=0.5", fc="yellow", alpha=0.5),
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=0"),
        )

        # Find and plot the spot of maximum profit
        max_profit_spot = spot_range[np.argmax(payoff)]
        plt.axvline(
            x=max_profit_spot, color="green", linestyle="--", label="Max Profit Spot"
        )
        plt.annotate(
            f"Max Profit Spot: {max_profit_spot:.2f}",
            xy=(max_profit_spot, plt.ylim()[1]),
            xytext=(5, -10),
            textcoords="offset points",
            ha="left",
            va="top",
            bbox=dict(boxstyle="round,pad=0.5", fc="lightgreen", alpha=0.5),
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=0"),
        )

        plt.title("Payoff Analysis")
        plt.xlabel("Spot Price")
        plt.ylabel("Profit/Loss (₹)")
        plt.legend()
        plt.grid(True)

        # Set y-axis limits to show actual profit/loss values
        y_min, y_max = np.min(payoff), np.max(payoff)
        y_range = y_max - y_min
        plt.ylim(y_min - 0.1 * y_range, y_max + 0.1 * y_range)

        # Format y-axis ticks to show actual values in thousands
        plt.gca().yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, p: f"{x/1000:.0f}K")
        )

        buf = BytesIO()
        plt.savefig(buf, format="png", dpi=300, bbox_inches="tight")
        buf.seek(0)
        img_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        self.payoff_chart.src = f"data:image/png;base64,{img_base64}"
        plt.close()

    def update_metrics(self):
        positions = self.get_positions_callback()
        if not positions:
            return

        total_mtm = sum(position.getPnL() for position in positions)
        underlying_price = self.strategy.getLastPrice(self.strategy.underlying)
        spot_range = np.linspace(underlying_price * 0.5, underlying_price * 1.5, 1000)
        payoff = self.calculate_payoff(positions, spot_range)
        max_profit = np.max(payoff)
        max_loss = np.min(payoff)

        greeks = self.strategy.getGreeks(
            [position.getInstrument() for position in positions]
        )

        total_delta = 0.0
        total_gamma = 0.0
        total_theta = 0.0
        total_vega = 0.0

        for position in positions:
            instrument = position.getInstrument()
            if instrument in greeks:
                greek = greeks[instrument]
                quantity = position.getEntryOrder().getQuantity()
                sign = 1 if position.getEntryOrder().isBuy() else -1
                total_delta += greek.delta * quantity * sign
                total_gamma += greek.gamma * quantity * sign
                total_theta += greek.theta * quantity * sign
                total_vega += greek.vega * quantity * sign

        # Calculate POP (Probability of Profit)
        pop = np.sum(payoff > 0) / len(payoff) * 100

        # Calculate Risk/Reward ratio
        risk_reward = abs(max_profit / max_loss) if max_loss != 0 else 0

        # Calculate Breakeven points
        breakeven_points = spot_range[np.where(np.diff(np.sign(payoff)))[0]]
        breakeven_str = ", ".join([f"{bp:.2f}" for bp in breakeven_points])

        # Update the metrics
        self.metrics.content.controls[0].controls[0].controls[
            1
        ].value = f"₹ {total_mtm:.2f}"
        self.metrics.content.controls[0].controls[1].controls[
            1
        ].value = f"₹ {max_profit:.2f}"
        self.metrics.content.controls[0].controls[2].controls[
            1
        ].value = f"₹ {max_loss:.2f}"

        self.metrics.content.controls[1].controls[0].controls[1].value = f"{pop:.2f}%"
        self.metrics.content.controls[1].controls[1].controls[
            1
        ].value = f"{risk_reward:.2f}"
        self.metrics.content.controls[1].controls[2].controls[
            1
        ].value = f"{breakeven_str} ({len(breakeven_points)})"

        self.metrics.content.controls[2].controls[0].controls[
            1
        ].value = f"{total_delta:.2f}"
        self.metrics.content.controls[2].controls[1].controls[
            1
        ].value = f"{total_gamma:.4f}"
        self.metrics.content.controls[2].controls[2].controls[
            1
        ].value = f"₹ {total_theta:.2f}"
        self.metrics.content.controls[2].controls[3].controls[
            1
        ].value = f"₹ {total_vega:.2f}"

    def updateData(self):
        if not self.__did_mount:
            return

        self.update_payoff_chart()
        self.update_metrics()
        self.update()
