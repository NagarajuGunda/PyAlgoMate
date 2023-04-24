import zmq.asyncio
import streamlit as st
import asyncio
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx
from streamlit.runtime.scriptrunner.script_run_context import get_script_run_ctx
from st_aggrid import GridOptionsBuilder, AgGrid, AgGridTheme, ColumnsAutoSizeMode

import pandas as pd
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
def get_placeholder():
    return st.empty()


def plotChart(df):
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
    print('Before subscribe')
    subscriber.subscribe(b"")
    while True:
        print('Before Recv')
        message = await subscriber.recv_json()
        message = json.loads(message)
        print('After Recv')
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
                    lists[strategy]["trades"] = pd.read_json(strategyData[key])
                elif key == "ohlc":
                    lists[strategy]["ohlc"] = pd.read_json(strategyData[key])
                else:
                    lists[strategy][key] = strategyData[key]
        displayData()


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
    if trades_df.shape[0] == 0:
        return

    # Define the strike price range for the options payoff chart
    midStrike = int((trades_df["Strike"].min() +
                    trades_df["Strike"].max()) / 2)
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


def plotOHLC(df):
    # Create the candlestick chart
    fig = go.Figure(data=[go.Candlestick(x=df['Date/Time'],
                                         open=df['Open'],
                                         high=df['High'],
                                         low=df['Low'],
                                         close=df['Close'])])

    # Add title and axis labels
    fig.update_layout(title=f'Bar Chart',
                      xaxis_title='Date',
                      yaxis_title='Price')

    # Display the chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)


def displayData():
    METRICS_PER_ROW = 3
    CHARTS_PER_ROW = 3

    get_placeholder().empty()
    with get_placeholder().container():
        strategies = (key for key in get_data().copy())
        selectedStrategy = st.selectbox(
            'Select a strategy', strategies, key=f"{datetime.datetime.now()}")
        if get_data().get(selectedStrategy, None) is None:
            return
        strategyData = get_data().copy()[selectedStrategy]
        st.subheader(f'Last updated time: {strategyData["datetime"]}')
        tab1, tab2, tab3, tab4, tab5 = st.tabs(
            ["Option Chain", "Trades", "Metrics", "Charts", "OHLC"])

        with tab1:
            if strategyData.get('optionChain', None) is not None:
                st.dataframe(strategyData['optionChain'].reset_index(
                    drop=True), use_container_width=True)
        with tab2:
            if strategyData.get('trades', None) is not None:
                tradesData = strategyData["trades"]
                optionChainData = strategyData["optionChain"]
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
                    if mergedData.shape[0] > 0:
                        plotPayOff(
                            mergedData[mergedData["Exit Price"].isnull()])
        with tab3:
            if strategyData.get('metrics', None) is not None:
                metricsData = strategyData["metrics"]
                col1, col2, col3 = st.columns(3)
                for index, metric in enumerate(metricsData):
                    with eval(f'col{(index % METRICS_PER_ROW) + 1}'):
                        st.metric(
                            label=metric, value=metricsData[metric], delta=metricsData[metric])
        with tab4:
            if strategyData.get('charts', None) is not None:
                chartData = strategyData["charts"]
                col1, col2, col3 = st.columns(3)
                for index, chart in enumerate(chartData):
                    with eval(f'col{(index % CHARTS_PER_ROW) + 1}'):
                        plotChart(pd.DataFrame(
                            chartData[chart], columns=[chart]))

        with tab5:
            if strategyData.get('ohlc', None) is not None:
                ohlcData = strategyData["ohlc"]

                tickers = ohlcData['Ticker'].unique().tolist()

                st.session_state.selectedTicker = st.selectbox(
                    'Select a Ticker', tickers, key=f"{datetime.datetime.now()}")

                plotOHLC(ohlcData[ohlcData['Ticker'] ==
                                  st.session_state.selectedTicker])


def startSubscriber():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(subscribe())


def run():
    displayData()

    subscriberThread = threading.Thread(target=startSubscriber)
    ctx = get_script_run_ctx()
    add_script_run_ctx(subscriberThread)
    subscriberThread.start()


def main():
    run()


if __name__ == "__main__":
    main()
