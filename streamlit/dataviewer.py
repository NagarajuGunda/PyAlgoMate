from pathlib import Path
import pandas as pd
import json
import numpy as np
import plotly.graph_objs as go
import streamlit as st
from streamlit_lightweight_charts import renderLightweightCharts


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
    COLOR_BULL = 'rgba(38,166,154,0.9)'  # 26a69a
    COLOR_BEAR = 'rgba(239,83,80,0.9)'  # #ef5350

    # Some data wrangling to match required format
    df = df.reset_index(drop=True)
    df.columns = ['time', 'open', 'high', 'low', 'close',
                  'volume']
    df['time'] = df['time'].apply(lambda x: int(x.timestamp()))

    # export to JSON format
    candles = json.loads(df.to_json(orient="records"))
    volume = json.loads(
        df.rename(columns={"volume": "value", }).to_json(orient="records"))

    chartMultipaneOptions = [
        {
            "height": 600,
            "layout": {
                "background": {
                    "type": "solid",
                    "color": 'white'
                },
                "textColor": "black"
            },
            "grid": {
                "vertLines": {
                    "color": "rgba(197, 203, 206, 0.5)"
                },
                "horzLines": {
                    "color": "rgba(197, 203, 206, 0.5)"
                }
            },
            "crosshair": {
                "mode": 0
            },
            "priceScale": {
                "borderColor": "rgba(197, 203, 206, 0.8)"
            },
            "timeScale": {
                'timeVisible': True,
                'secondsVisible': True,
                "borderColor": "rgba(197, 203, 206, 0.8)",
                "barSpacing": 15
            },
            "watermark": {
                "visible": True,
                "fontSize": 48,
                "horzAlign": 'center',
                "vertAlign": 'center',
                "color": 'rgba(171, 71, 188, 0.3)',
                "text": ticker,
            }
        },
        {
            "height": 100,
            "layout": {
                "background": {
                    "type": 'solid',
                    "color": 'transparent'
                },
                "textColor": 'black',
            },
            "grid": {
                "vertLines": {
                    "color": 'rgba(42, 46, 57, 0)',
                },
                "horzLines": {
                    "color": 'rgba(42, 46, 57, 0.6)',
                }
            },
            "timeScale": {
                "visible": False,
            },
            "watermark": {
                "visible": True,
                "fontSize": 18,
                "horzAlign": 'left',
                "vertAlign": 'top',
                "color": 'rgba(171, 71, 188, 0.7)',
                "text": 'Volume',
            }
        }
    ]

    seriesCandlestickChart = [
        {
            "type": 'Candlestick',
            "data": candles,
            "options": {
                "upColor": COLOR_BULL,
                "downColor": COLOR_BEAR,
                "borderVisible": False,
                "wickUpColor": COLOR_BULL,
                "wickDownColor": COLOR_BEAR
            }
        }
    ]
    seriesVolumeChart = [
        {
            "type": 'Histogram',
            "data": volume,
            "options": {
                "priceFormat": {
                    "type": 'volume',
                },
                "priceScaleId": ""  # set as an overlay setting,
            },
            "priceScale": {
                "scaleMargins": {
                    "top": 0,
                    "bottom": 0,
                },
                "alignLabels": False
            }
        }
    ]

    renderLightweightCharts([
        {
            "chart": chartMultipaneOptions[0],
            "series": seriesCandlestickChart
        },
        {
            "chart": chartMultipaneOptions[1],
            "series": seriesVolumeChart
        }
    ], 'multipane')

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

        def updateStraddleCharts():
            st.session_state["oldStraddleCharts"] = st.session_state['straddleCharts']
            st.session_state["straddleCharts"] = st.session_state.newStraddleCharts

        tickerColumn, dateRangeColumn, timeframeColumn = st.columns([6, 2, 2])

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

        with timeframeColumn:
            st.session_state["selectedTimeFrame"] = st.selectbox(
                "Select a Timeframe",
                [1, 3, 5, 10, 15, 30, 60],
                key="newTimeFrame",
                on_change=updateTimeFrame
            )

        print(f'Selected timeframe - {st.session_state["selectedTimeFrame"]}')

        st.session_state['straddleCharts'] = st.checkbox(
            "Straddle charts", key='newStraddleCharts', on_change=updateStraddleCharts)

        if st.session_state['straddleCharts']:
            pattern = r'([A-Z\|]+)(\d{2})([A-Z]{3})(\d{2})([CP])(\d+)'
            filteredData['Ticker'] = filteredData[filteredData['Ticker'].str.match(
                pattern)]['Ticker'].str.replace(pattern, r'\1 \2\3\4 \6 ATM', regex=True)

            filteredData = filteredData[~filteredData.Ticker.isna()]

            filteredData = filteredData.groupby(by=['Date/Time', 'Ticker']).agg({
                'Open': 'sum',
                'High': 'sum',
                'Low': 'sum',
                'Close': 'sum',
                'Volume': 'sum',
                'Open Interest': 'sum'
            }).reset_index()

        with tickerColumn:
            tickers = filteredData["Ticker"].unique().tolist()

            if "selectedTicker" not in st.session_state:
                st.session_state["selectedTicker"] = tickers[0]
            if "oldTicker" not in st.session_state:
                st.session_state["oldTicker"] = ""

            if st.session_state["selectedTicker"] not in tickers:
                st.session_state["tickers"] = tickers[0] if len(
                    tickers) > 0 else None

            st.session_state["selectedTicker"] = st.selectbox(
                "Select a Ticker",
                tickers,
                key="newTicker",
                on_change=updateTicker
            )

        print(f"Selected Ticker - {st.session_state['selectedTicker']}")

        filteredData = filteredData[filteredData["Ticker"]
                                    == st.session_state["selectedTicker"]]
        plotCandlestickChart(
            filteredData, st.session_state["selectedTicker"], st.session_state["selectedTimeFrame"])


if __name__ == "__main__":
    main()
