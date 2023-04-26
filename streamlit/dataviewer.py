from st_aggrid import GridUpdateMode, DataReturnMode
import streamlit as st
import pandas as pd
from pathlib import Path

###################################
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import JsCode

###################################
import plotly.graph_objs as go


def plotOHLC(dfIn: pd.DataFrame):
    timeframe = '5min'

    # if df['Date/Time'].dtype == 'datetime64[ns]':
    #     df['Date/Time'] = df['Date/Time'].dt.strftime("%Y/%m/%d %H:%M")

    dfIn['Date/Time'] = pd.to_datetime(dfIn['Date/Time'])

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
    dataframeContainer = st.expander("Check filtered dataframe")
    dataframeContainer.write(df)


st.set_page_config(page_icon="✂️", page_title="CSV Reader", layout="wide")

c29, c30, c31 = st.columns([1, 6, 1])

with c30:

    uploaded_file = st.file_uploader(
        "",
        key="1",
        help="To activate 'wide mode', go to the hamburger menu > Settings > turn on 'wide mode'",
    )

    if uploaded_file is not None:
        file_container = st.expander("Check your uploaded .csv/.parquet")
        ohlcData = pd.read_parquet(uploaded_file) if Path(
            uploaded_file.name).suffix == '.parquet' else pd.read_csv(uploaded_file)

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

        def tickerCallback():
            st.session_state["oldTicker"] = st.session_state["selectedTicker"]
            st.session_state["selectedTicker"] = st.session_state.newTicker

        st.session_state.selectedTicker = st.selectbox(
            'Select a Ticker', tickers, key="newTicker",
            on_change=tickerCallback)

        print(f'Selected Ticker - {st.session_state.selectedTicker}')

        ohlcData = ohlcData[ohlcData['Ticker'] ==
                            st.session_state.selectedTicker]
        # file_container.write(ohlcData)
        plotOHLC(ohlcData)

    else:
        st.info(
            f"""
                👆 Upload a .csv/.parquet file first.
                """
        )

        st.stop()


# gb = GridOptionsBuilder.from_dataframe(ohlcData)
# # enables pivoting on all columns, however i'd need to change ag grid to allow export of pivoted/grouped data, however it select/filters groups
# gb.configure_default_column(
#     enablePivot=True, enableValue=True, enableRowGroup=True)
# gb.configure_selection(selection_mode="multiple", use_checkbox=True)
# gb.configure_side_bar()  # side_bar is clearly a typo :) should by sidebar
# gridOptions = gb.build()

# st.success(
#     f"""
#         💡 Tip! Hold the shift key when selecting rows to select multiple rows at once!
#         """
# )

# response = AgGrid(
#     ohlcData,
#     gridOptions=gridOptions,
#     enable_enterprise_modules=True,
#     update_mode=GridUpdateMode.MODEL_CHANGED,
#     data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
#     fit_columns_on_grid_load=False,
# )

# df = pd.DataFrame(response["selected_rows"])

# st.subheader("Filtered data will appear below 👇 ")
# st.text("")

# st.table(df)

st.text("")
