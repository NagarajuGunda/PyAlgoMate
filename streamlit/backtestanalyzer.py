import pandas as pd
import numpy as np
import plotly.graph_objs as go
import plotly.express as px
import streamlit as st


# Calculate Winning and Losing Streaks


def GetStreaks(tradesData):
    win_streak = 0
    loss_streak = 0
    max_win_streak = 0
    max_loss_streak = 0
    prev_trade_profit = 0
    for trade_profit in tradesData['PnL']:
        if trade_profit > 0:
            win_streak += 1
            loss_streak = 0
            if win_streak > max_win_streak:
                max_win_streak = win_streak
        elif trade_profit < 0:
            loss_streak += 1
            win_streak = 0
            if loss_streak > max_loss_streak:
                max_loss_streak = loss_streak
        else:
            win_streak = 0
            loss_streak = 0
        prev_trade_profit = trade_profit

    return max_win_streak, max_loss_streak


# Calculate Expectancy
def GetExpectancy(tradesData):
    winningTrades = tradesData[tradesData['PnL'] > 0]['PnL']
    losingTrades = tradesData[tradesData['PnL'] < 0]['PnL']

    winningTradesAvg = winningTrades.mean()
    losingTradesAvg = losingTrades.mean()

    winningTradesCount = len(winningTrades)
    losingTradesCount = len(losingTrades)

    totalTrades = winningTradesCount + losingTradesCount
    winningTradesPct = winningTradesCount / totalTrades
    losingTradesPct = losingTradesCount / totalTrades

    expectancy = (abs(winningTradesAvg / losingTradesAvg)
                  * winningTradesPct) - losingTradesPct

    return expectancy


def box(col, key, value, percentage=None, color='green'):
    with col:
        style = """
            <style>
            .average__card {
                position: relative;
                padding: 0;
                height: 100%;
                border: 1px solid green;
                border-radius: 4px;
                text-align: center;
                overflow: hidden;
            }
            .average__card .__title {
                padding: 5px 0;
                border-bottom: 1px solid;
                border-radius: 4px 4px 0 0;
                color: green;
                background-color: rgba(0, 128, 0, 0.15);
            }
            .average__card .__value {
                padding: 10px 0;
            }
            .__average__price {
                border-color: #f19f15;
            }
            .__average__price .__title {
                color: #f19f15;
                background-color: #fef8e1;
            }
            .__loss__price {
                border-color: #ef9a99;
            }
            .__loss__price .__title {
                color: red;
                border-color: #ef9a99;
                background-color: #fbebee;
            }
            .stats_percent {
                font-size: 13px;
                opacity: 0.8;
            }
            </style>
        """

        subclass = '__average__price' if color == 'yellow' else (
            '__loss__price' if color == 'red' else '')
        percent = f'<span class="stats_percent">({percentage})</span>' if percentage is not None else ''
        st.markdown(
            f'<div class="average__card {subclass}"><div class="__title">{key}</div><div class="__value">{value} {percent}</div></div>{style}',
            unsafe_allow_html=True
        )


def showStats(initialCapital: int, tradesData: pd.DataFrame):
    overallPnL = tradesData['PnL'].sum()
    averageProfit = tradesData['PnL'].mean()
    maxProfit = tradesData['PnL'].max()
    maxLoss = tradesData['PnL'].min()

    col1, col2, col3, col4, col5 = st.columns(5, gap='small')
    box(col1, 'Initial Capital', f'₹{initialCapital}')
    box(col2, 'Overall Profit/Loss',
        f'₹{round(overallPnL, 2)}', f'{round((overallPnL/initialCapital)*100, 2)}%')
    box(col3, 'Average Day Profit', f'₹{round(averageProfit, 2)}',
        f'{round((averageProfit/initialCapital)*100, 2)}%', color='yellow')
    box(col4, 'Max Profit', f'₹{round(maxProfit, 2)}',
        f'{round((maxProfit/initialCapital)*100, 2)}%')
    box(col5, 'Max Loss', f'₹{round(maxLoss, 2)}',
        f'{round((maxLoss/initialCapital)*100, 2)}%', color='red')
    st.write('')

    wins = tradesData['PnL'][tradesData['PnL'] > 0].count()
    losses = tradesData['PnL'][tradesData['PnL'] < 0].count()
    totalCount = tradesData['PnL'].count()
    winPercentage = (wins / totalCount) * 100
    lossPercentage = (losses / totalCount) * 100
    monthlyProfit = tradesData.resample(
        'M', on='Date').sum(numeric_only=True)['PnL'].mean()
    averageProfitOnWins = tradesData['PnL'][tradesData['PnL'] > 0].mean()
    averageLossOnLosses = tradesData['PnL'][tradesData['PnL'] < 0].mean()

    col1, col2, col3, col4, col5 = st.columns(5, gap='small')
    box(col1, 'Win% (Days)', f'{round(winPercentage, 2)} ({wins})')
    box(col2, 'Loss% (Days)',
        f'{round(lossPercentage, 2)} ({losses})', color='red')
    box(col3, 'Avg Monthly Profit', '₹{:.2f}'.format(
        monthlyProfit), f'{round((monthlyProfit/initialCapital)*100, 2)}%', color='yellow')
    box(col4, 'Avg Profit On Win Days', '₹{:.2f}'.format(
        averageProfitOnWins), f'{round((averageProfitOnWins/initialCapital)*100, 2)}%', initialCapital)
    box(col5, 'Avg Loss On Loss Days', '₹{:.2f}'.format(
        averageLossOnLosses), f'{round((averageLossOnLosses/initialCapital)*100, 2)}%', color='red')
    st.write('')

    cumulativePnL = tradesData['PnL'].cumsum()
    runningMaxPnL = cumulativePnL.cummax()
    drawdown = cumulativePnL - runningMaxPnL
    mdd = drawdown.min()

    # Find the index of the maximum drawdown
    mddIndex = drawdown.idxmin()

    # Find the index of the peak before the maximum drawdown
    peakIndex = cumulativePnL[:mddIndex].idxmax()

    # Compute the date range corresponding to the maximum drawdown
    mddStartDate = tradesData['Date'][peakIndex]
    mddEndDate = tradesData['Date'][mddIndex]
    mddDateRange = f"{mddStartDate.strftime('%d %b %Y')} - {mddEndDate.strftime('%d %b %Y')}"
    mddDays = (mddEndDate - mddStartDate).days

    # Calculate the Return to MDD ratio
    averageYearlyProfit = tradesData.set_index(
        'Date')['PnL'].cumsum().resample('Y').last().diff().mean()
    returnToMddRatio = abs(averageYearlyProfit / mdd)

    col1, col2, col3 = st.columns(3, gap='small')
    box(col1, 'Max Drawdown (MDD)',
        f'{mdd:.2f}', f'{(mdd/initialCapital)*100:.2f}%', color='red')
    box(col2, 'MDD Days (Recovery Days)',
        f'{mddDays}', mddDateRange, color='red')
    box(col3, 'Return to MDD Ratio',
        'Requires minimum 1Yr data' if returnToMddRatio is not None else f'{returnToMddRatio:.2f}')
    st.write('')

    maxWinningStreak, maxLosingStreak = GetStreaks(tradesData)
    expectancy = GetExpectancy(tradesData)

    col1, col2, col3 = st.columns(3, gap='small')
    box(col1, 'Max Winning Streak', f'{maxWinningStreak}')
    box(col2, 'Max Losing Streak',
        f'{maxLosingStreak}', color='red')
    box(col3, 'Expectancy', f'{expectancy:.2f}')
    st.write('')


def plotScatterMAE(tradesData):
    tradesData[['MAE', 'MFE', 'PnL']] = tradesData[[
        'MAE', 'MFE', 'PnL']].fillna(0)

    mae = tradesData['MAE']
    pnl = tradesData['PnL']

    # Check for collinearity
    correlation_matrix = tradesData[['MAE', 'MFE', 'PnL']].corr()
    if correlation_matrix.iloc[0, 1] > 0.9:
        st.error(
            "High collinearity detected between MAE and MFE. Consider removing one of the variables.")
        return

    # Handle missing or invalid data
    tradesData[['MAE', 'MFE', 'PnL']] = tradesData[[
        'MAE', 'MFE', 'PnL']].fillna(0)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=mae,
        y=pnl,
        mode='markers',
        name='MAE',
        marker=dict(color=np.where(pnl < 0, 'red', 'green'))
    ))
    fig.update_layout(
        title='MAE vs PnL',
        xaxis_title='MAE',
        yaxis_title='PnL'
    )
    st.plotly_chart(fig, use_container_width=True)


def plotScatterMFE(tradesData):
    tradesData[['MAE', 'MFE', 'PnL']] = tradesData[[
        'MAE', 'MFE', 'PnL']].fillna(0)

    mfe = tradesData['MFE']
    pnl = tradesData['PnL']

    # Check for collinearity
    correlation_matrix = tradesData[['MAE', 'MFE', 'PnL']].corr()
    if correlation_matrix.iloc[0, 1] > 0.9:
        st.error(
            "High collinearity detected between MAE and MFE. Consider removing one of the variables.")
        return

    # Handle missing or invalid data
    tradesData[['MAE', 'MFE', 'PnL']] = tradesData[[
        'MAE', 'MFE', 'PnL']].fillna(0)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=mfe,
        y=pnl,
        mode='markers',
        name='MFE',
        marker=dict(color=np.where(pnl < 0, 'red', 'green'))
    ))
    fig.update_layout(
        title='MFE vs PnL',
        xaxis_title='MFE',
        yaxis_title='PnL'
    )
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.set_page_config(page_title="Backtest Analyzer", layout="wide")
    col1, col, col2 = st.columns([1, 8, 1])

    with col:
        uploadedFile = st.file_uploader(
            "",
            key="1",
            help="To activate 'wide mode', go to the hamburger menu > Settings > turn on 'wide mode'",
        )
        if uploadedFile is None:
            st.info("👆 Upload a backtest csv file first.")
            st.stop()

        tradesData = pd.read_csv(uploadedFile)
        tradesData["Entry Date/Time"] = pd.to_datetime(
            tradesData["Entry Date/Time"])
        tradesData["Exit Date/Time"] = pd.to_datetime(
            tradesData["Exit Date/Time"])
        tradesData["Date"] = pd.to_datetime(tradesData["Date"])
        uploadedFile.seek(0)

        groupCol, dateRangeCol, daysCol = st.columns([1, 5, 3])

        with groupCol:
            selectedGroupCriteria = st.selectbox(
                'Group by', ('Date', 'Day', 'Trade', 'Expiry'))

        initialCapital = st.text_input('Initial Capital', placeholder='200000')
        if initialCapital is not None and initialCapital != '':
            initialCapital = int(float(initialCapital))
        else:
            return

        if selectedGroupCriteria is not None:
            if selectedGroupCriteria == 'Date':
                groupBy = tradesData.groupby('Date')
                xAxisTitle = 'Date'
                showStats(initialCapital, groupBy['PnL'].sum().reset_index())
            elif selectedGroupCriteria == 'Day':
                groupBy = tradesData.groupby(
                    tradesData['Date'].dt.strftime('%A'))
                xAxisTitle = 'Day'
                showStats(initialCapital, tradesData)
            else:
                groupBy = None
                xAxisTitle = selectedGroupCriteria
                showStats(initialCapital, tradesData)

            if groupBy is not None:
                pnl = groupBy['PnL'].sum()
                groupByDate = tradesData.groupby(
                    'Date')['PnL'].sum().reset_index()
                cumulativePnL = groupByDate['PnL'].cumsum()
                cumulativePnL.index = groupByDate['Date']
            else:
                pnl = tradesData['PnL']
                pnl.index = tradesData['Entry Date/Time']
                cumulativePnL = tradesData['PnL'].cumsum()
                cumulativePnL.index = tradesData['Entry Date/Time']
                runningMaxPnL = cumulativePnL.cummax()

            runningMaxPnL = cumulativePnL.cummax()
            drawdown = cumulativePnL - runningMaxPnL
            drawdown.index = cumulativePnL.index

            with st.empty():
                st.header('PnL')
                color = ['green' if x > 0 else 'red' for x in pnl]
                fig = go.Figure(
                    data=[go.Bar(x=pnl.index, y=pnl.values, marker={'color': color})])
                fig.update_layout(title='PnL over time',
                                  xaxis_title=xAxisTitle, yaxis_title='PnL')
                st.plotly_chart(fig, use_container_width=True)

            with st.empty():
                st.header('Cumulative PnL')
                fig = go.Figure(
                    data=[go.Scatter(x=cumulativePnL.index, y=cumulativePnL)])
                fig.update_layout(title='Cumulative PnL over time',
                                  xaxis_title='Date', yaxis_title='Cumulative PnL')
                st.plotly_chart(fig, use_container_width=True)

            with st.empty():
                st.header('Drawdown')
                fig = go.Figure(
                    data=[go.Scatter(x=drawdown.index, y=drawdown)])
                fig.update_layout(
                    title='Drawdown', xaxis_title='Date', yaxis_title='Drawdown')
                st.plotly_chart(fig, use_container_width=True)

            with st.empty():
                st.header('MAE vs PnL')
                plotScatterMAE(tradesData)

            with st.empty():
                st.header('MFE vs PnL')
                plotScatterMFE(tradesData)

        with st.expander("Check dataframe"):
            st.dataframe(tradesData, use_container_width=True)


if __name__ == "__main__":
    main()
