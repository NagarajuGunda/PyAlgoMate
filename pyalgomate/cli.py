import click
import zmq
import json
import logging
import datetime
import glob
import pandas as pd
import pyalgomate.utils as utils
import inspect
from pyalgomate.telegram import TelegramBot

# ZeroMQ Context
context = zmq.Context()

# Define the socket using the "Context"
sock = context.socket(zmq.PUB)


def createStrategyInstance(strategyClass, argsDict):
    # Get the parameters of the strategy class
    parameters = inspect.signature(strategyClass).parameters

    # Prepare the arguments for the constructor
    constructorArgs = {}
    for paramName, param in parameters.items():
        if paramName in argsDict:
            # Use the provided value if available
            constructorArgs[paramName] = argsDict[paramName]
        elif param.default != inspect.Parameter.empty:
            # Use the default value if available
            constructorArgs[paramName] = param.default
        else:
            # Parameter is required but not provided, raise an exception or handle the case as needed
            raise ValueError(
                f"Missing value for required parameter: {paramName}")

    # Create an instance of the strategy class with the prepared arguments
    return strategyClass(**constructorArgs)


def valueChangedCallback(strategy, value):
    jsonDump = json.dumps({strategy: value})
    sock.send_json(jsonDump)


@click.group()
@click.pass_context
def cli(ctx):
    global strategyClass
    ctx.obj = strategyClass


def checkDate(ctx, param, value):
    try:
        if value is None:
            return value

        _ = datetime.datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise click.UsageError("Not a valid date: '{0}'.".format(value))


def getDataFrameFromParquets(dataFiles, startDate=None, endDate=None):
    df = None
    for files in dataFiles:
        for file in glob.glob(files):
            if df is None:
                df = pd.read_parquet(file)
            else:
                df = pd.concat([df, pd.read_parquet(file)],
                               ignore_index=True)

    df = df.sort_values(['Ticker', 'Date/Time']).drop_duplicates(
        subset=['Ticker', 'Date/Time'], keep='first')

    return df


def backtest(strategyClass, completeDf, df, underlyings, send_to_ui, telegramBot, load_all):
    from pyalgomate.backtesting import DataFrameFeed, CustomCSVFeed
    from pyalgomate.brokers import BacktestingBroker

    start = datetime.datetime.now()
    feed = None
    if load_all:
        feed = CustomCSVFeed.CustomCSVFeed()
        for underlying in underlyings:
            feed.addBarsFromDataframe(df, underlying)
    else:
        feed = DataFrameFeed.DataFrameFeed(completeDf, df, underlyings, )

    print(f"Time took in loading the data <{datetime.datetime.now() - start}>")

    broker = BacktestingBroker(200000, feed)

    argsDict = {
        'feed': feed,
        'broker': broker,
        'underlying': underlyings[0],
        'underlyings': underlyings,
        'lotSize': 15,
        'callback': valueChangedCallback if send_to_ui else None,
        'telegramBot': telegramBot
    }

    strategy = createStrategyInstance(strategyClass, argsDict)
    try:
        strategy.run()
    except Exception as e:
        click.echo(f'Exception occurred while running {strategy.strategyName}. Error <{e}>')

    return strategy.getTrades()


@cli.command(name='backtest')
@click.option('--underlying', default=['BANKNIFTY'], multiple=True, help='Specify an underlying')
@click.option('--data', prompt='Specify data file', multiple=True)
@click.option('--port', help='Specify a zeroMQ port to send data to', default=5680, type=click.INT)
@click.option('--send-to-ui', help='Specify if data needs to be sent to UI', default=False, type=click.BOOL)
@click.option('--send-to-telegram', help='Specify if messages needs to be sent to telegram', default=False,
              type=click.BOOL)
@click.option('--from-date', help='Specify a from date', callback=checkDate, default=None, type=click.STRING)
@click.option('--to-date', help='Specify a to date', callback=checkDate, default=None, type=click.STRING)
@click.option('--parallelize', help='Specify if backtest in parallel', default=None,
              type=click.Choice(['Day', 'Month']))
@click.option('--load-all', help='Specify if all the data needs to be loaded', default=False, type=click.BOOL)
@click.pass_obj
def runBacktest(strategyClass, underlying, data, port, send_to_ui, send_to_telegram, from_date, to_date, parallelize,
                load_all):
    import yaml
    from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
    import multiprocessing
    import pandas as pd
    import os

    if len(underlying) == 0:
        underlying = ['BANKNIFTY']

    underlyings = list(underlying)

    if send_to_ui:
        sock.bind(f"tcp://127.0.0.1:{port}")

    underlyings = list(underlying)

    if send_to_telegram:
        with open('cred.yml') as f:
            creds = yaml.load(f, Loader=yaml.FullLoader)
            telegramBot = TelegramBot(
                creds['Telegram']['token'], creds['Telegram']['chatid'],
                creds['Telegram']['allow'] if 'allow' in creds['Telegram'] else [])
    else:
        telegramBot = None

    constructorArgs = inspect.signature(strategyClass.__init__).parameters
    argNames = [param for param in constructorArgs]
    click.echo(f"{strategyClass.__name__} takes {argNames}")

    df = getDataFrameFromParquets(dataFiles=data)
    startDate = datetime.datetime.strptime(
        from_date, "%Y-%m-%d").date() if from_date is not None else None
    endDate = datetime.datetime.strptime(
        to_date, "%Y-%m-%d").date() if to_date is not None else None

    completeDf = df

    if startDate:
        df = df[df['Date/Time'].dt.date >= startDate]
    if endDate:
        df = df[df['Date/Time'].dt.date <= endDate]

    if parallelize == 'Day':
        groups = df.groupby(
            [df['Date/Time'].dt.year, df['Date/Time'].dt.month, df['Date/Time'].dt.date])
    elif parallelize == 'Month':
        groups = df.groupby(
            [df['Date/Time'].dt.year, df['Date/Time'].dt.month])
    else:
        parallelize = None

    start = datetime.datetime.now()

    backtestResults = []

    workers = multiprocessing.cpu_count()
    if parallelize:
        print(f"Running with {workers} workers")

        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = []
            for groupKey, groupDf in groups:
                future = executor.submit(
                    backtest, strategyClass, None, groupDf, underlyings, send_to_ui, telegramBot, load_all)
                futures.append(future)

            for future in futures:
                results = future.result()
                backtestResults.append(results)

        tradesDf = pd.DataFrame()
        for backtestResult in backtestResults:
            tradesDf = pd.concat([tradesDf, backtestResult], ignore_index=True)
    else:
        tradesDf = backtest(strategyClass, completeDf, df,
                            underlyings, send_to_ui, telegramBot, load_all)

    print("")
    print(
        f"Time took in running the strategy <{datetime.datetime.now() - start}>")

    tradesDf.sort_values(by=['Entry Date/Time'])
    tradesDf.to_csv(f'results/{strategyClass.__name__}_backtest.csv', mode='a',
                    header=not os.path.exists(f'results/{strategyClass.__name__}_backtest.csv'), index=False)

    if telegramBot:
        telegramBot.stop()  # Signal the stop event
        telegramBot.waitUntilFinished()
        telegramBot.delete()  # Delete the TelegramBot instance


@cli.command(name='trade')
@click.option('--broker', prompt='Select a broker', type=click.Choice(['Finvasia', 'Zerodha']), help='Select a broker')
@click.option('--mode', prompt='Select a trading mode', type=click.Choice(['paper', 'live']),
              help='Select a trading mode')
@click.option('--underlying', multiple=True, help='Specify an underlying')
@click.option('--collect-data', help='Specify if the data needs to be collected to data.csv', default=False,
              type=click.BOOL)
@click.option('--port', help='Specify a zeroMQ port to send data to', default=5680, type=click.INT)
@click.option('--send-to-ui', help='Specify if data needs to be sent to UI', default=False, type=click.BOOL)
@click.option('--send-to-telegram', help='Specify if messages needs to be sent to telegram', default=False,
              type=click.BOOL)
@click.option('--register-options',
              help='Specify which expiry options to register. Allowed values are Weekly, NextWeekly, Monthly',
              default=["Weekly"], type=click.STRING, multiple=True)
@click.option('--send-logs', help='Specify if logs needs to be sent to papertrail', default=False, type=click.BOOL)
@click.pass_obj
def runLiveTrade(strategyClass, broker, mode, underlying, collect_data, port, send_to_ui, send_to_telegram,
                 register_options, send_logs):
    if not broker:
        raise click.UsageError('Please select a broker')

    if not mode:
        raise click.UsageError('Please select a mode')

    if send_to_ui:
        sock.bind(f"tcp://127.0.0.1:{port}")

    import yaml
    import pyotp
    import datetime
    import os

    click.echo(
        f'broker <{broker}> mode <{mode}> underlying <{underlying}> collect-data <{collect_data}> port <{port}> send-to-ui <{send_to_ui}> send-to-telegram <{send_to_telegram}> register-options <{register_options}> send-logs <{send_logs}>')

    underlyings = list(underlying)

    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    if send_logs:
        import socket
        from logging.handlers import SysLogHandler

        if 'PaperTrail' not in creds:
            click.echo('Error: PaperTrail creds not found.')
            exit()

        papertrailCreds = creds['PaperTrail']['address'].split(':')

        class ContextFilter(logging.Filter):
            hostname = socket.gethostname()

            def filter(self, record):
                record.hostname = ContextFilter.hostname
                return True

        syslog = SysLogHandler(address=(papertrailCreds[0], int(papertrailCreds[1])))
        syslog.addFilter(ContextFilter())
        _format = '%(asctime)s %(hostname)s PyAlgoMate: %(message)s'
        formatter = logging.Formatter(_format, datefmt='%b %d %H:%M:%S')
        syslog.setFormatter(formatter)
        logger = logging.getLogger()
        logger.addHandler(syslog)
        logger.setLevel(logging.INFO)

    optionSymbols = []

    if broker == 'Finvasia':
        import pyalgomate.brokers.finvasia as finvasia
        from pyalgomate.brokers import getBroker

        barFeed, api = finvasia.getFeed(creds[broker], register_options, underlyings)
        broker = getBroker(barFeed, api, broker, mode)
    elif broker == 'Zerodha':
        from pyalgomate.brokers.zerodha.kiteext import KiteExt
        import pyalgomate.brokers.zerodha as zerodha
        from pyalgomate.brokers.zerodha.broker import getZerodhaTokensList
        from pyalgomate.brokers.zerodha.feed import ZerodhaLiveFeed
        from pyalgomate.brokers.zerodha.broker import ZerodhaPaperTradingBroker, ZerodhaLiveBroker

        cred = creds[broker]

        api = KiteExt()
        twoFA = pyotp.TOTP(cred['factor2']).now()
        api.login_with_credentials(
            userid=cred['user'], password=cred['pwd'], twofa=twoFA)

        profile = api.profile()
        print(f"Welcome {profile.get('user_name')}")

        currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
            datetime.datetime.now().date())
        nextWeekExpiry = utils.getNextWeeklyExpiryDate(
            datetime.datetime.now().date())
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(
            datetime.datetime.now().date())

        if len(underlyings) == 0:
            underlyings = ['NSE:NIFTY BANK']

        for underlying in underlyings:
            ltp = api.quote(underlying)[
                underlying]["last_price"]

            if "Weekly" in register_options:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, currentWeeklyExpiry, ltp, 10)
            if "NextWeekly" in register_options:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, nextWeekExpiry, ltp, 10)
            if "Monthly" in register_options:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, monthlyExpiry, ltp, 10)

        optionSymbols = list(dict.fromkeys(optionSymbols))

        tokenMappings = getZerodhaTokensList(
            api, underlyings + optionSymbols)

        barFeed = ZerodhaLiveFeed(api, tokenMappings)

        if mode == 'paper':
            broker = ZerodhaPaperTradingBroker(200000, barFeed)
        else:
            broker = ZerodhaLiveBroker(api)

    telegramBot = None
    if send_to_telegram and 'Telegram' in creds:
        telegramBot = TelegramBot(
            creds['Telegram']['token'], creds['Telegram']['chatid'],
            creds['Telegram']['allow'] if 'allow' in creds['Telegram'] else [])

    constructorArgs = inspect.signature(strategyClass.__init__).parameters
    argNames = [param for param in constructorArgs]
    click.echo(f"{strategyClass.__name__} takes {argNames}")

    argsDict = {
        'feed': barFeed,
        'broker': broker,
        'underlying': underlyings[0],
        'underlyings': underlyings,
        'registeredOptionsCount': len(optionSymbols),
        'lotSize': 15,
        'callback': valueChangedCallback if send_to_ui else None,
        'collectData': collect_data,
        'telegramBot': telegramBot
    }

    strategy = createStrategyInstance(strategyClass, argsDict)
    strategy.run()

    if telegramBot:
        telegramBot.stop()  # Signal the stop event
        telegramBot.waitUntilFinished()
        telegramBot.delete()  # Delete the TelegramBot instance


def CliMain(cls):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[%(levelname)s]|[%(asctime)s]|[%(process)d::%(thread)d]|[%(name)s::%(module)s::%(funcName)s::%(lineno)d]|=> "
        "%(message)s"
    )

    fileHandler = logging.FileHandler('PyAlgoMate.log')
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(formatter)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    consoleHandler.setFormatter(formatter)

    logger.addHandler(fileHandler)
    logger.addHandler(consoleHandler)

    logging.getLogger("requests").setLevel(logging.WARNING)

    try:
        global strategyClass
        strategyClass = cls
        cli(standalone_mode=False)
    except click.UsageError as e:
        click.echo(f'Error: {str(e)}')
        click.echo(cli.get_help())
