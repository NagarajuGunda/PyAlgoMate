import zmq.asyncio
import streamlit as st
import asyncio
from st_aggrid import GridOptionsBuilder, AgGrid, AgGridTheme, GridUpdateMode, DataReturnMode, ColumnsAutoSizeMode

import pandas as pd
import altair as alt
import numpy as np
import plotly.express as px
import json
import datetime
import plotly.graph_objs as go
import re
from streamlit_plotly_events import plotly_events

st.set_page_config(layout="wide")


@st.cache_resource
def get_data():
    lists = {}
    return lists


@st.cache_resource
def get_placeholders():
    placeholders = {}
    return placeholders


def plotChart(df):
    # df = pd.DataFrame([10, 15, 5, 0, -5, -10, 0, 10,
    #                   20, 25, 20, 30], columns=["pnl"])

    # Add an index column to the DataFrame
    df = df.reset_index().rename(columns={'index': 'index'})

    y = df.columns[-1]
    df[y] = pd.to_numeric(df[y])
    df["color"] = np.where(df[y] < 0, 'loss', 'profit')

    fig = px.area(df, x="index", y=y, color="color", color_discrete_map={'loss': 'orangered',
                                                                         'profit': 'lightgreen'})
    fig.for_each_trace(lambda trace: trace.update(fillcolor=trace.line.color))

    st.plotly_chart(fig, use_container_width=True)


async def subscribe():
    context = zmq.asyncio.Context.instance()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://127.0.0.1:5680")
    subscriber.subscribe(b"")
    while True:
        message = await subscriber.recv_json()
        message = json.loads(message)
        print(message)
        lists = get_data()
        for strategy in message:
            strategyData = message[strategy]
            if lists.get(strategy, None) is None:
                lists[strategy] = {}
            for key in strategyData:
                if key == "metrics":
                    metrics = strategyData[key]
                    if lists[strategy].get("metrics", None) is None:
                        lists[strategy]["metrics"] = {}
                    for metric in metrics:
                        lists[strategy]["metrics"][metric] = metrics[metric]
                elif key == "charts":
                    charts = strategyData[key]
                    if lists[strategy].get("charts", None) is None:
                        lists[strategy]["charts"] = {}
                    for chart in charts:
                        if lists[strategy]["charts"].get(chart, None) is None:
                            lists[strategy]["charts"][chart] = []
                        lists[strategy]["charts"][chart].append(charts[chart])
                elif key == 'optionChain':
                    optionChainDict = strategyData[key]
                    df = pd.DataFrame.from_dict(
                        optionChainDict, orient='index')
                    columnsList = optionChainDict[next(
                        iter(optionChainDict))].keys()
                    df.columns = [column.capitalize()
                                  for column in columnsList]
                    df = df[['Symbol', 'Strike', 'Expiry', 'Price',
                             'Delta', 'Gamma', 'Theta', 'Vega', 'Iv']]
                    lists[strategy]["optionChain"] = df
                elif key == "trades":
                    print(strategyData[key])
                    lists[strategy]["trades"] = pd.read_json(strategyData[key])
                else:
                    lists[strategy][key] = strategyData[key]
                display_data(strategy, key)


async def start_subscriber():
    await subscribe()


def displayDataframe(data, key):
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_pagination(paginationPageSize=10)  # Add pagination
    gb.configure_side_bar()  # Add a sidebar
    gridOptions = gb.build()

    grid_response = AgGrid(
        data,
        gridOptions=gridOptions,
        data_return_mode='AS_INPUT',
        update_mode='MODEL_CHANGED',
        fit_columns_on_grid_load=False,
        # Only choices: AgGridTheme.STREAMLIT, AgGridTheme.ALPINE, AgGridTheme.BALHAM, AgGridTheme.MATERIAL
        theme=AgGridTheme.STREAMLIT,
        reload_data=True,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        key=key + datetime.datetime.now().isoformat()
    )


def getOptionType(instrument):
    m = re.match(r"([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)", instrument)

    if m is None:
        return None

    return m.group(5)

def plotPayOff(dataframe: pd.DataFrame):
    # Load the trades dataframe
    trades_df = dataframe

    # Define the strike price range for the options payoff chart
    midStrike = int((trades_df["Strike"].min() + trades_df["Strike"].max()) / 2)
    minStrike = midStrike - (20 * 100)
    maxStrike = midStrike + (20 * 100)
    strikes = list(range(minStrike, maxStrike+100, 100))

    # Create a DataFrame to store the payoff for each underlying
    payoff_df = pd.DataFrame({'Underlying': strikes})

    for underlying in strikes:
        # Calculate the payoff for each individual option trade
        payoffs = []
        for i, trade in trades_df.iterrows():
            if trade['Buy/Sell'] == 'Buy':
                if trade['Strike'] > underlying:
                    if getOptionType(trade['Instrument']) == 'C':
                        payoffs.append(-(trade['Entry Price']
                                       * trade['Quantity']))
                    else:
                        payoffs.append(
                            (trade['Strike'] - underlying - trade['Entry Price']) * trade['Quantity'])
                else:
                    if getOptionType(trade['Instrument']) == 'C':
                        payoffs.append(
                            (underlying - trade['Strike'] - trade['Entry Price']) * trade['Quantity'])
                    else:
                        payoffs.append(-(trade['Entry Price']
                                       * trade['Quantity']))
            else:
                if trade['Strike'] > underlying:
                    if getOptionType(trade['Instrument']) == 'C':
                        payoffs.append(trade['Entry Price']
                                       * trade['Quantity'])
                    else:
                        payoffs.append(
                            (-(trade['Strike'] - underlying) + trade['Entry Price']) * trade['Quantity'])
                else:
                    if getOptionType(trade['Instrument']) == 'C':
                        payoffs.append(
                            (-(underlying - trade['Strike']) + trade['Entry Price']) * trade['Quantity'])
                    else:
                        payoffs.append(trade['Entry Price']
                                       * trade['Quantity'])

        # Calculate the total payoff for the portfolio at the given underlying price
        total_payoff = sum(payoffs)

        # Add the total payoff to the DataFrame
        payoff_df.loc[payoff_df['Underlying'] ==
            underlying, 'Payoff'] = total_payoff

    # Create a plotly figure object
    fig = go.Figure()

    # Add a scatter plot for the payoff line
    fig.add_trace(go.Scatter(x=payoff_df['Underlying'], y=payoff_df['Payoff'],
                             mode='lines', name='Payoff', line=dict(width=3, color='black')))

    # Add a trace for the profit/loss region
    profit_loss = payoff_df.copy()
    profit_loss['Payoff'] = np.where(
        payoff_df['Payoff'] > 0, payoff_df['Payoff'], 0)
    loss = payoff_df.copy()
    loss['Payoff'] = np.where(payoff_df['Payoff'] < 0, payoff_df['Payoff'], 0)

    fig.add_trace(go.Scatter(x=profit_loss['Underlying'], y=profit_loss['Payoff'],
                             fill='tozeroy', mode='none', name='Profit', line=dict(width=0),
                             fillcolor='rgba(60, 179, 113, 0.4)', showlegend=True))

    fig.add_trace(go.Scatter(x=loss['Underlying'], y=loss['Payoff'],
                             fill='tozeroy', mode='none', name='Loss', line=dict(width=0),
                             fillcolor='rgba(178, 34, 34, 0.4)', showlegend=True))

    # Set the plot layout
    fig.update_layout(title='Option Payoff',
                      xaxis_title='Underlying Price', yaxis_title='Payoff')

    # Display the plotly figure object
    st.plotly_chart(fig, use_container_width=True)


def display_data(strategy, key):
    placeholders = get_placeholders()
    data = get_data().copy()

    if (len(placeholders) == 0):
        return

    if key == "optionChain" or key == "all":
        with placeholders["optionChain"]:
            # displayDataframe(data[strategy]["optionChain"], "optionChain")
            st.dataframe(data[strategy]["optionChain"].reset_index(
                drop=True), use_container_width=True)
    if key == "trades" or key == "all":
        with placeholders["trades"]:
            tradesData = data[strategy]["trades"]
            optionChainData = data[strategy]["optionChain"]
            mergedData = pd.merge(tradesData, optionChainData.rename(columns={
                                  "Symbol": "Instrument", "Price": "LTP"}), on="Instrument", how="inner")
            reOrderedColumns = ["Instrument", "Buy/Sell", "Entry Date/Time", "Exit Date/Time", "Quantity", "Entry Price", "Exit Price", "LTP",
                                "Strike", "Expiry", "Delta", "Gamma", "Theta", "Vega", "Iv"]
            mergedData = mergedData[reOrderedColumns]

            overallGreeks = mergedData[mergedData["Exit Price"].isnull(
            )][["Delta", "Gamma", "Vega", "Theta", "Iv"]].aggregate("sum").to_json()

            tab1, tab2 = st.tabs(["Trades", "Payoff"])
            with tab1:
                st.dataframe(mergedData, use_container_width=True)
            with tab2:
                plotPayOff(mergedData[mergedData["Exit Price"].isnull()])
    if key == "metrics" or key == "all":
        for metric in data[strategy]["metrics"]:
            value = data[strategy]["metrics"][metric]
            placeHolderKey = "metrics_" + metric
            if placeholders.get(placeHolderKey, None) is None:
                continue
            with placeholders[placeHolderKey]:
                st.metric(label=metric, value=value, delta=value)
    if key == "charts" or key == "all":
        for chart in data[strategy]["charts"]:
            values = data[strategy]["charts"][chart]
            placeHolderKey = "charts_" + chart
            if placeholders.get(placeHolderKey, None) is None:
                continue
            with placeholders[placeHolderKey]:
                plotChart(pd.DataFrame(values, columns=[chart]))


def main():
    METRICS_PER_ROW = 3
    CHARTS_PER_ROW = 3
    data = get_data().copy()
    placeholders = get_placeholders()
    strategies = (key for key in data)
    strategySelection = st.selectbox(
        'Select a strategy',
        strategies)

    # Set up page content based on sidebar selection
    if data.get(strategySelection, None) is not None:
        tab1, tab2, tab3, tab4 = st.tabs(
            ["Option Chain", "Trades", "Metrics", "Charts"])

        for tab in ["optionChain", "trades", "metrics", "charts"]:
            if placeholders.get(tab, None):
                placeholders[tab].empty()

        with tab1:
            placeholders["optionChain"] = st.empty()
        with tab2:
            placeholders["trades"] = st.empty()
        with tab3:
            if placeholders.get("metrics", None):
                placeholders["metrics"].empty()
            placeholders["metrics"] = st.empty()
            metrics = data[strategySelection]["metrics"]
            with placeholders["metrics"]:
                col1, col2, col3 = st.columns(3)
                for index, metric in enumerate(metrics):
                    with eval(f'col{(index % METRICS_PER_ROW) + 1}'):
                        placeholders["metrics_" + metric] = st.empty()
        with tab4:
            if placeholders.get("charts", None):
                placeholders["charts"].empty()
            placeholders["charts"] = st.empty()
            charts = data[strategySelection]["charts"]
            with placeholders["charts"]:
                col1, col2, col3 = st.columns(3)
                for index, chart in enumerate(charts):
                    with eval(f'col{(index % CHARTS_PER_ROW) + 1}'):
                        placeholders["charts_" + chart] = st.empty()

    display_data(strategySelection, "all")
    asyncio.run(start_subscriber())


if __name__ == "__main__":
    main()
