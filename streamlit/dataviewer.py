from pathlib import Path
import pandas as pd
import plotly.graph_objs as go
import streamlit as st


def plotCandlestickChart(df, ticker, timeframe):
    if df.empty:
        st.write("No data found!")
        return

    timeframe = f"{timeframe}min"
    df = (
        df.resample(timeframe, on="Date/Time")
          .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
          .reset_index()
          .dropna()
    )
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["Date/Time"],
                open=df["Open"],
                high=df["High"],
                low=df["Low"],
                close=df["Close"]
            )
        ]
    )
    dtAll = pd.date_range(
        start=df["Date/Time"].iloc[0], end=df["Date/Time"].iloc[-1], freq=timeframe)
    dtObs = [d.strftime("%Y-%m-%d %H:%M:%S") for d in df["Date/Time"]]
    dtBreaks = [d for d in dtAll.strftime(
        "%Y-%m-%d %H:%M:%S").tolist() if d not in dtObs]
    dtBreaks = pd.to_datetime(dtBreaks)
    fig.update_xaxes(rangebreaks=[dict(dvalue=5 * 60 * 1000, values=dtBreaks)])

    # Add title and axis labels
    fig.update_layout(title=ticker, xaxis_title="Date/Time",
                      yaxis_title="Price")

    # Display the chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)
    with st.expander("Check filtered dataframe"):
        st.dataframe(df, use_container_width=True)


def main():
    st.set_page_config(page_icon="✂️", page_title="CSV Reader", layout="wide")
    col1, col, col2 = st.columns([1, 20, 1])

    with col:
        uploadedFile = st.file_uploader(
            "",
            key="1",
            help="To activate 'wide mode', go to the hamburger menu > Settings > turn on 'wide mode'",
        )
        if uploadedFile is None:
            st.info("👆 Upload a .csv/.parquet file first.")
            st.stop()

        ohlcData = pd.read_parquet(uploadedFile) if Path(
            uploadedFile.name).suffix == ".parquet" else pd.read_csv(uploadedFile)
        ohlcData["Date/Time"] = pd.to_datetime(ohlcData["Date/Time"])
        uploadedFile.seek(0)

        tickers = ohlcData["Ticker"].unique().tolist()

        if "selectedTicker" not in st.session_state:
            st.session_state["selectedTicker"] = tickers[0]
        if "oldTicker" not in st.session_state:
            st.session_state["oldTicker"] = ""

        if st.session_state["selectedTicker"] not in tickers:
            st.session_state["tickers"] = tickers[0]

        def updateTicker():
            st.session_state["oldTicker"] = st.session_state["selectedTicker"]
            st.session_state["selectedTicker"] = st.session_state.newTicker

        def updateDateRange():
            st.session_state["oldDateRange"] = st.session_state["selectedDateRange"]
            st.session_state["selectedDateRange"] = st.session_state.newDateRange

        def updateTimeFrame():
            st.session_state["oldTimeFrame"] = st.session_state.get(
                "selectedTimeFrame", "1")
            st.session_state["selectedTimeFrame"] = st.session_state.newTimeFrame

        tickerColumn, dateRangeColumn, timeframeColumn = st.columns([6, 2, 2])
        with tickerColumn:
            st.session_state["selectedTicker"] = st.selectbox(
                "Select a Ticker",
                tickers,
                key="newTicker",
                on_change=updateTicker
            )

        print(f"Selected Ticker - {st.session_state['selectedTicker']}")

        dates = ohlcData["Date/Time"].agg(["min", "max"])
        with dateRangeColumn:
            st.session_state["selectedDateRange"] = st.date_input(
                label="Select date range",
                min_value=dates["min"],
                max_value=dates["max"],
                value=dates["max"],
                key="newDateRange",
                on_change=updateDateRange
            )
        print(f"Selected date range - {st.session_state['selectedDateRange']}")
        filteredData = pd.DataFrame()
        if "selectedDateRange" in st.session_state:
            if isinstance(st.session_state["selectedDateRange"], tuple) and len(st.session_state["selectedDateRange"]) == 2:
                filteredData = ohlcData[
                    (ohlcData["Date/Time"].dt.date >=
                     st.session_state["selectedDateRange"][0])
                    & (ohlcData["Date/Time"].dt.date <= st.session_state["selectedDateRange"][1])
                ]
            else:
                filteredData = ohlcData[ohlcData["Date/Time"].dt.date ==
                                        st.session_state["selectedDateRange"]]

        filteredData = filteredData[filteredData["Ticker"]
                                    == st.session_state["selectedTicker"]]

        with timeframeColumn:
            st.session_state["selectedTimeFrame"] = st.selectbox(
                "Select a Timeframe",
                [1, 3, 5, 10, 15, 30, 60],
                key="newTimeFrame",
                on_change=updateTimeFrame
            )

        print(f'Selected timeframe - {st.session_state["selectedTimeFrame"]}')
        plotCandlestickChart(
            filteredData, st.session_state["selectedTicker"], st.session_state["selectedTimeFrame"])


if __name__ == "__main__":
    main()
