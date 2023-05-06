from st_aggrid import GridUpdateMode, DataReturnMode
import streamlit as st
import pandas as pd
from pathlib import Path
import datetime

###################################
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode

###################################
import plotly.graph_objs as go


def plotOHLC(dfIn: pd.DataFrame, timeframe):
    if dfIn.shape[0] == 0:
        st.write('No data found!')
        return

    timeframe = f'{timeframe}min'

    # if df['Date/Time'].dtype == 'datetime64[ns]':
    #     df['Date/Time'] = df['Date/Time'].dt.strftime("%Y/%m/%d %H:%M")

    df = dfIn.resample(timeframe, on='Date/Time').agg(
        {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).reset_index().dropna()
    # Create the candlestick chart
    fig = go.Figure(data=[go.Candlestick(x=df['Date/Time'],
                                         open=df['Open'],
                                         high=df['High'],
                                         low=df['Low'],
                                         close=df['Close'])])

    # grab first and last observations from df.date and make a continuous date range from that
    dt_all = pd.date_range(
        start=df['Date/Time'].iloc[0], end=df['Date/Time'].iloc[-1], freq=timeframe)

    # check which dates from your source that also accur in the continuous date range
    dt_obs = [d.strftime("%Y-%m-%d %H:%M:%S") for d in df['Date/Time']]

    # isolate missing timestamps
    dt_breaks = [d for d in dt_all.strftime(
        "%Y-%m-%d %H:%M:%S").tolist() if not d in dt_obs]
    dt_breaks = pd.to_datetime(dt_breaks)

    fig.update_xaxes(rangebreaks=[dict(dvalue=5*60*1000, values=dt_breaks)])

    # Add title and axis labels
    fig.update_layout(title=f'{dfIn.Ticker.values[0]}',
                      xaxis_title='Date/Time',
                      yaxis_title='Price')

    # Display the chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Check filtered dataframe"):
        st.dataframe(df, use_container_width=True)


st.set_page_config(page_icon="✂️", page_title="CSV Reader", layout="wide")

c29, c30, c31 = st.columns([1, 20, 1])

with c30:

    uploaded_file = st.file_uploader(
        "",
        key="1",
        help="To activate 'wide mode', go to the hamburger menu > Settings > turn on 'wide mode'",
    )

    if uploaded_file is not None:
        ohlcData = pd.read_parquet(uploaded_file) if Path(
            uploaded_file.name).suffix == '.parquet' else pd.read_csv(uploaded_file)
        ohlcData['Date/Time'] = pd.to_datetime(ohlcData['Date/Time'])
        uploaded_file.seek(0)

        tickers = ohlcData['Ticker'].unique().tolist()
        # Check if session state object exists
        if "selectedTicker" not in st.session_state:
            st.session_state['selectedTicker'] = tickers[0]
        if 'oldTicker' not in st.session_state:
            st.session_state['oldTicker'] = ""

        # Check if value exists in the new options list. if it does retain the selection, else reset
        if st.session_state["selectedTicker"] not in tickers:
            st.session_state["tickers"] = tickers[0]

        oldTicker = st.session_state["oldTicker"]

        def updateTicker():
            st.session_state["oldTicker"] = st.session_state["selectedTicker"]
            st.session_state["selectedTicker"] = st.session_state.newTicker

        def updateDateRange():
            st.session_state['oldDateRange'] = st.session_state['selectedDateRange']
            st.session_state['selectedDateRange'] = st.session_state.newDateRange

        def updateTimeFrame():
            st.session_state['oldTimeFrame'] = st.session_state.get(
                'selectedTimeFrame', '1')
            st.session_state['selectedTimeFrame'] = st.session_state.newTimeFrame

        tickerColumn, dateRangeColumn, timeframeColumn = st.columns([6, 2, 2])
        with tickerColumn:
            st.session_state.selectedTicker = st.selectbox(
                'Select a Ticker', tickers, key="newTicker",
                on_change=updateTicker)

        print(f'Selected Ticker - {st.session_state.selectedTicker}')

        dates = ohlcData['Date/Time'].agg(['min', 'max'])

        with dateRangeColumn:
            st.session_state.selectedDateRange = st.date_input(
                label="Select date range",
                min_value=dates['min'],
                max_value=dates['max'],
                value=dates['max'],
                key="newDateRange",
                on_change=updateDateRange
            )

        print(f'Selected date range - {st.session_state.selectedDateRange}')

        if 'selectedDateRange' in st.session_state:
            if isinstance(st.session_state.selectedDateRange, tuple) and len(st.session_state.selectedDateRange) == 2:
                filteredData = ohlcData[(ohlcData['Date/Time'].dt.date >= st.session_state.selectedDateRange[0]) & (
                    ohlcData['Date/Time'].dt.date <= st.session_state.selectedDateRange[1])]
            else:
                filteredData = ohlcData[ohlcData['Date/Time'].dt.date ==
                                        st.session_state.selectedDateRange]

        filteredData = filteredData[filteredData['Ticker'] ==
                                    st.session_state.selectedTicker]

        with timeframeColumn:
            st.session_state.selectedTicker = st.selectbox(
                'Select a Timeframe', [1, 3, 5, 10, 15, 30, 60], key="newTimeFrame",
                on_change=updateTimeFrame)

        plotOHLC(filteredData, st.session_state.selectedTicker)
    else:
        st.info(
            f"""
                👆 Upload a .csv/.parquet file first.
                """
        )

        st.stop()

st.text("")
