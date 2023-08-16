
import click
import zmq
import json
import logging
import datetime
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


def backtest(strategyClass, df, underlyings, send_to_ui, telegramBot, results):
    from pyalgomate.backtesting import CustomCSVFeed
    from pyalgomate.brokers import BacktestingBroker

    feed = CustomCSVFeed.CustomCSVFeed()

    for underlying in underlyings:
        feed.addBarsFromDataframe(df, underlying)

    broker = BacktestingBroker(200000, feed)

    argsDict = {
        'feed': feed,
        'broker': broker,
        'underlying': underlyings[0],
        'underlyings': underlyings,
        'lotSize': 25,
        'collectTrades': False,
        'callback': valueChangedCallback if send_to_ui else None,
        'telegramBot': telegramBot
    }

    strategy = createStrategyInstance(strategyClass, argsDict)
    strategy.run()
    results.append(strategy.getTrades())


@cli.command(name='backtest')
@click.option('--underlying', default=['BANKNIFTY'], multiple=True, help='Specify an underlying')
@click.option('--data', prompt='Specify data file', multiple=True)
@click.option('--port', help='Specify a zeroMQ port to send data to', default=5680, type=click.INT)
@click.option('--send-to-ui', help='Specify if data needs to be sent to UI', default=False, type=click.BOOL)
@click.option('--send-to-telegram', help='Specify if messages needs to be sent to telegram', default=False, type=click.BOOL)
@click.option('--from-date', help='Specify a from date', callback=checkDate,  default=None, type=click.STRING)
@click.option('--to-date', help='Specify a to date', callback=checkDate, default=None, type=click.STRING)
@click.option('--parallelize', help='Specify if backtest in parallel', default=None, type=click.Choice(['Day', 'Month']))
@click.pass_obj
def runBacktest(strategyClass, underlying, data, port, send_to_ui, send_to_telegram, from_date, to_date, parallelize):
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

    from pyalgomate.backtesting import CustomCSVFeed

    underlyings = list(underlying)

    if send_to_telegram:
        with open('cred.yml') as f:
            creds = yaml.load(f, Loader=yaml.FullLoader)
            telegramBot = TelegramBot(
                creds['Telegram']['token'], creds['Telegram']['chatid'])
    else:
        telegramBot = None

    constructorArgs = inspect.signature(strategyClass.__init__).parameters
    argNames = [param for param in constructorArgs]
    click.echo(f"{strategyClass.__name__} takes {argNames}")

    feed = CustomCSVFeed.CustomCSVFeed()

    df = feed.getDataFrameFromParquets(dataFiles=data,
                                       startDate=datetime.datetime.strptime(
                                           from_date, "%Y-%m-%d").date() if from_date is not None else None,
                                       endDate=datetime.datetime.strptime(to_date, "%Y-%m-%d").date() if to_date is not None else None)

    if parallelize == 'Day':
        groups = df.groupby(
            [df['Date/Time'].dt.year, df['Date/Time'].dt.month, df['Date/Time'].dt.date])
    elif parallelize == 'Month':
        groups = df.groupby(
            [df['Date/Time'].dt.year, df['Date/Time'].dt.month])
    else:
        groups = [(None, df)]

    start = datetime.datetime.now()

    backtestResults = []

    workers = multiprocessing.cpu_count()
    print(f"Running with {workers} workers")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for groupKey, groupDf in groups:
            results = []
            future = executor.submit(
                backtest, strategyClass, groupDf, underlyings, send_to_ui, telegramBot, results)
            futures.append((future, results))

        for future, results in futures:
            future.result()
            backtestResults.extend(results)

    tradesDf = pd.DataFrame()
    for backtestResult in backtestResults:
        tradesDf = pd.concat([tradesDf, backtestResult], ignore_index=True)

    tradesDf.sort_values(by=['Entry Date/Time'])
    tradesDf.to_csv(f'results/{strategyClass.__name__}_backtest.csv', mode='a',
                    header=not os.path.exists(f'results/{strategyClass.__name__}_backtest.csv'), index=False)

    print("")
    print(
        f"Time took in running the strategy <{datetime.datetime.now()-start}>")

    if telegramBot:
        telegramBot.stop()  # Signal the stop event
        telegramBot.waitUntilFinished()
        telegramBot.delete()  # Delete the TelegramBot instance


@cli.command(name='trade')
@click.option('--broker', prompt='Select a broker', type=click.Choice(['Finvasia', 'Zerodha']), help='Select a broker')
@click.option('--mode', prompt='Select a trading mode', type=click.Choice(['paper', 'live']), help='Select a trading mode')
@click.option('--underlying', multiple=True, help='Specify an underlying')
@click.option('--collect-data', help='Specify if the data needs to be collected to data.csv', default=False, type=click.BOOL)
@click.option('--port', help='Specify a zeroMQ port to send data to', default=5680, type=click.INT)
@click.option('--send-to-ui', help='Specify if data needs to be sent to UI', default=False, type=click.BOOL)
@click.option('--send-to-telegram', help='Specify if messages needs to be sent to telegram', default=False, type=click.BOOL)
@click.option('--register-options', help='Specify which expiry options to register. Allowed values are Weekly, NextWeekly, Monthly', default=["Weekly"], type=click.STRING, multiple=True)
@click.pass_obj
def runLiveTrade(strategyClass, broker, mode, underlying, collect_data, port, send_to_ui, send_to_telegram, register_options):
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

    click.echo(f'broker <{broker}> mode <{mode}> underlying <{underlying}> collect-data <{collect_data}> port <{port}> send-to-ui <{send_to_ui}> send-to-telegram <{send_to_telegram}> register-options <{register_options}>')

    underlyings = list(underlying)

    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    if broker == 'Finvasia':
        from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
        from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker, getFinvasiaToken, getFinvasiaTokenMappings
        import pyalgomate.brokers.finvasia as finvasia
        from pyalgomate.brokers.finvasia.feed import LiveTradeFeed

        cred = creds[broker]

        api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                         websocket='wss://api.shoonya.com/NorenWSTP/')
        userToken = None
        tokenFile = 'shoonyakey.txt'
        if os.path.exists(tokenFile) and (datetime.datetime.fromtimestamp(os.path.getmtime(tokenFile)).date() == datetime.datetime.today().date()):
            click.echo(f"Token has been created today already. Re-using it")
            with open(tokenFile, 'r') as f:
                userToken = f.read()
            click.echo(
                f"userid {cred['user']} password ******** usertoken {userToken}")
            loginStatus = api.set_session(
                userid=cred['user'], password=cred['pwd'], usertoken=userToken)
        else:
            click.echo(f"Logging in and persisting user token")
            loginStatus = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

            if loginStatus:
                with open(tokenFile, 'w') as f:
                    f.write(loginStatus.get('susertoken'))

                click.echo(
                    f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")
            else:
                click.echo(f'Login failed!')

        if loginStatus != None:
            currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
                datetime.datetime.now().date())
            nextWeekExpiry = utils.getNextWeeklyExpiryDate(
                datetime.datetime.now().date())
            monthlyExpiry = utils.getNearestMonthlyExpiryDate(
                datetime.datetime.now().date())

            if len(underlyings) == 0:
                underlyings = ['NSE|NIFTY BANK']

            optionSymbols = []

            for underlying in underlyings:
                ltp = api.get_quotes('NSE', getFinvasiaToken(
                    api, underlying))['lp']

                if "Weekly" in register_options:
                    optionSymbols += finvasia.broker.getOptionSymbols(
                        underlying, currentWeeklyExpiry, ltp, 10)
                if "NextWeekly" in register_options:
                    optionSymbols += finvasia.broker.getOptionSymbols(
                        underlying, nextWeekExpiry, ltp, 10)
                if "Monthly" in register_options:
                    optionSymbols += finvasia.broker.getOptionSymbols(
                        underlying, monthlyExpiry, ltp, 10)

            optionSymbols = list(dict.fromkeys(optionSymbols))

            tokenMappings = getFinvasiaTokenMappings(
                api, underlyings + optionSymbols)

            barFeed = LiveTradeFeed(api, tokenMappings)

            if mode == 'paper':
                broker = PaperTradingBroker(200000, barFeed)
            else:
                broker = LiveBroker(api)
        else:
            exit(1)
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

        optionSymbols = []

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

    if send_to_telegram:
        telegramBot = TelegramBot(
            creds['Telegram']['token'], creds['Telegram']['chatid'])
    else:
        telegramBot = None

    constructorArgs = inspect.signature(strategyClass.__init__).parameters
    argNames = [param for param in constructorArgs]
    click.echo(f"{strategyClass.__name__} takes {argNames}")

    argsDict = {
        'feed': barFeed,
        'broker': broker,
        'underlying': underlyings[0],
        'underlyings': underlyings,
        'registeredOptionsCount': len(optionSymbols),
        'lotSize': 25,
        'collectTrades': True,
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
    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(filename=f'{cls.__name__}.log', level=logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)

    try:
        global strategyClass
        strategyClass = cls
        cli(standalone_mode=False)
    except click.UsageError as e:
        click.echo(f'Error: {str(e)}')
        click.echo(cli.get_help())
