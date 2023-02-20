import zmq.asyncio
import streamlit as st
import asyncio
import pandas as pd
import altair as alt
import numpy as np
import plotly.express as px
import json

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
                else:
                    lists[strategy][key] = strategyData[key]
            display_data(strategy)


async def start_subscriber():
    await subscribe()


def display_data(strategy):
    option = st.session_state.get('selected_option', None)
    placeholders = get_placeholders()
    data = get_data().copy()
    if option == "Metrics":
        for key in data[strategy]["metrics"]:
            value = data[strategy]["metrics"][key]
            with placeholders["metrics_" + key]:
                st.metric(label=key, value=value, delta=value)
    if option == "Charts":
        for key in data[strategy]["charts"]:
            values = data[strategy]["charts"][key]
            with placeholders["charts_" + key]:
                plotChart(pd.DataFrame(values, columns=[key]))
        

def option_selection():
    option = st.sidebar.selectbox('Select a page', ("Metrics", "Charts"))
    st.session_state.selected_option = option

def main():
    data = get_data().copy()
    placeholders = get_placeholders()
    strategies = (key for key in data)
    METRICS_PER_ROW = 3
    # Set up sidebar
    strategySelection = st.sidebar.selectbox(
        'Select a strategy',
        strategies)

    # Set up page content based on sidebar selection
    if data.get(strategySelection, None) is not None:
        option_selection()
        option = st.session_state.get('selected_option', None)
        if option == 'Metrics':
            placeholders.clear()
            metrics = data[strategySelection]["metrics"]
            col1, col2, col3 = st.columns(3)
            for index, metric in enumerate(metrics):
                with eval(f'col{(index % METRICS_PER_ROW) + 1}'):
                    placeholders["metrics_" + metric] = st.empty()
        elif option == 'Charts':
            placeholders.clear()
            charts = data[strategySelection]["charts"]
            col1, col2, col3 = st.columns(3)
            for index, chart in enumerate(charts):
                with eval(f'col{(index % METRICS_PER_ROW) + 1}'):
                    placeholders["charts_" + chart] = st.empty()

    display_data(strategySelection)
    asyncio.run(start_subscriber())


if __name__ == "__main__":
    main()
