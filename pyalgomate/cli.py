
import click
import zmq
import json
import logging
import datetime
import pyalgomate.utils as utils
import inspect

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


@cli.command(name='backtest')
@click.option('--underlying', default=['BANKNIFTY'], multiple=True, help='Specify an underlying')
@click.option('--data', prompt='Specify data file', multiple=True)
@click.option('--port', help='Specify a zeroMQ port to send data to', default=5680, type=click.INT)
@click.option('--send-to-ui', help='Specify if data needs to be sent to UI', default=False, type=click.BOOL)
@click.option('--from-date', help='Specify a from date', callback=checkDate,  default=None, type=click.STRING)
@click.option('--to-date', help='Specify a to date', callback=checkDate, default=None, type=click.STRING)
@click.pass_obj
def runBacktest(strategyClass, underlying, data, port, send_to_ui, from_date, to_date):
    if send_to_ui:
        sock.bind(f"tcp://127.0.0.1:{port}")

    from pyalgomate.backtesting import CustomCSVFeed
    from pyalgomate.brokers import BacktestingBroker

    underlyings = underlying
    if len(underlying) == 1:
        underlying = underlying[0]
    elif len(underlying) == 0:
        underlying = None

    start = datetime.datetime.now()
    feed = CustomCSVFeed.CustomCSVFeed()
    feed.addBarsFromParquets(
        dataFiles=data, ticker=underlying, startDate=datetime.datetime.strptime(from_date, "%Y-%m-%d").date() if from_date is not None else None, endDate=datetime.datetime.strptime(to_date, "%Y-%m-%d").date() if to_date is not None else None)

    print("")
    print(f"Time took in loading data <{datetime.datetime.now()-start}>")
    start = datetime.datetime.now()

    broker = BacktestingBroker(200000, feed)

    constructorArgs = inspect.signature(strategyClass.__init__).parameters
    argNames = [param for param in constructorArgs]
    click.echo(f"{strategyClass.__name__} takes {argNames}")

    argsDict = {
        'feed': feed,
        'broker': broker,
        'underlying': underlying,
        'underlyings': underlyings,
        'lotSize': 25,
        'callback': valueChangedCallback if send_to_ui else None
    }

    strategy = createStrategyInstance(strategyClass, argsDict)
    strategy.run()

    print("")
    print(
        f"Time took in running the strategy <{datetime.datetime.now()-start}>")


@cli.command(name='trade')
@click.option('--broker', prompt='Select a broker', type=click.Choice(['Finvasia', 'Zerodha']), help='Select a broker')
@click.option('--mode', prompt='Select a trading mode', type=click.Choice(['paper', 'live']), help='Select a trading mode')
@click.option('--underlying', multiple=True, help='Specify an underlying')
@click.option('--collect-data', help='Specify if the data needs to be collected to data.csv', default=True, type=click.BOOL)
@click.option('--port', help='Specify a zeroMQ port to send data to', default=5680, type=click.INT)
@click.option('--send-to-ui', help='Specify if data needs to be sent to UI', default=True, type=click.BOOL)
@click.option('--register-options', help='Specify which expiry options to register. Allowed values are Weekly, NextWeekly, Monthly', default=["Weekly"], type=click.STRING, multiple=True)
@click.pass_obj
def runLiveTrade(strategyClass, broker, mode, underlying, collect_data, port, send_to_ui, register_options):
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

    logger = logging.getLogger(__file__)

    click.echo(f'broker <{broker}> mode <{mode}> underlying <{underlying}> collect-data <{collect_data}> port <{port}> send-to-ui <{send_to_ui}> register-options <{register_options}>')

    underlyings = underlying
    if len(underlying) == 1:
        underlying = underlying[0]
    elif len(underlying) == 0:
        underlying = None

    with open('cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)

    if broker == 'Finvasia':
        from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
        from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker, getFinvasiaToken, getFinvasiaTokenMappings
        import pyalgomate.brokers.finvasia as finvasia
        from pyalgomate.brokers.finvasia.feed import LiveTradeFeed

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

            with open(tokenFile, 'w') as f:
                f.write(loginStatus.get('susertoken'))

            click.echo(
                f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")

        if loginStatus != None:
            underlyingInstrument = 'NSE|NIFTY BANK' if underlying is None else underlying

            ltp = api.get_quotes('NSE', getFinvasiaToken(
                api, underlyingInstrument))['lp']

            currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
                datetime.datetime.now().date())
            nextWeekExpiry = utils.getNextWeeklyExpiryDate(
                datetime.datetime.now().date())
            monthlyExpiry = utils.getNearestMonthlyExpiryDate(
                datetime.datetime.now().date())

            if "Weekly" in register_options:
                optionSymbols = finvasia.broker.getOptionSymbols(
                    underlyingInstrument, currentWeeklyExpiry, ltp, 10)
            if "NextWeekly" in register_options:
                optionSymbols += finvasia.broker.getOptionSymbols(
                    underlyingInstrument, nextWeekExpiry, ltp, 10)
            if "Monthly" in register_options:
                optionSymbols += finvasia.broker.getOptionSymbols(
                    underlyingInstrument, monthlyExpiry, ltp, 10)

            optionSymbols = list(dict.fromkeys(optionSymbols))

            tokenMappings = getFinvasiaTokenMappings(
                api, ["NSE|NIFTY INDEX", underlyingInstrument] + optionSymbols)

            barFeed = LiveTradeFeed(api, tokenMappings)

            if mode == 'paper':
                broker = PaperTradingBroker(200000, barFeed)
            else:
                broker = LiveBroker(api)
    elif broker == 'Zerodha':
        from pyalgomate.brokers.zerodha.kiteext import KiteExt
        import pyalgomate.brokers.zerodha as zerodha
        from pyalgomate.brokers.zerodha.broker import getZerodhaTokensList
        from pyalgomate.brokers.zerodha.feed import ZerodhaLiveFeed
        from pyalgomate.brokers.zerodha.broker import ZerodhaPaperTradingBroker, ZerodhaLiveBroker

        api = KiteExt()
        twoFA = pyotp.TOTP(cred['factor2']).now()
        api.login_with_credentials(
            userid=cred['user'], password=cred['pwd'], twofa=twoFA)

        profile = api.profile()
        print(f"Welcome {profile.get('user_name')}")

        underlyingInstrument = 'NSE:NIFTY BANK' if underlying is None else underlying

        ltp = api.quote(underlyingInstrument)[
            underlyingInstrument]["last_price"]

        currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
            datetime.datetime.now().date())
        nextWeekExpiry = utils.getNextWeeklyExpiryDate(
            datetime.datetime.now().date())
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(
            datetime.datetime.now().date())

        if "Weekly" in register_options:
            optionSymbols = zerodha.broker.getOptionSymbols(
                underlyingInstrument, currentWeeklyExpiry, ltp, 10)
        if "NextWeekly" in register_options:
            optionSymbols += zerodha.broker.getOptionSymbols(
                underlyingInstrument, nextWeekExpiry, ltp, 10)
        if "Monthly" in register_options:
            optionSymbols += zerodha.broker.getOptionSymbols(
                underlyingInstrument, monthlyExpiry, ltp, 10)

        optionSymbols = list(dict.fromkeys(optionSymbols))

        tokenMappings = getZerodhaTokensList(
            api, [underlyingInstrument] + optionSymbols)

        barFeed = ZerodhaLiveFeed(api, tokenMappings)

        if mode == 'paper':
            broker = ZerodhaPaperTradingBroker(200000, barFeed)
        else:
            broker = ZerodhaLiveBroker(api)

    constructorArgs = inspect.signature(strategyClass.__init__).parameters
    argNames = [param for param in constructorArgs]
    click.echo(f"{strategyClass.__name__} takes {argNames}")

    argsDict = {
        'feed': barFeed,
        'broker': broker,
        'underlying': underlyingInstrument,
        'underlyings': underlyings,
        'registeredOptionsCount': len(optionSymbols),
        'lotSize': 25,
        'callback': valueChangedCallback if send_to_ui else None,
        'collectData': collect_data
    }

    strategy = createStrategyInstance(strategyClass, argsDict)
    strategy.run()


def CliMain(cls):
    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(filename=f'{cls.__name__}.log', level=logging.INFO)

    try:
        global strategyClass
        strategyClass = cls
        cli()
    except click.UsageError as e:
        click.echo(f'Error: {str(e)}')
        click.echo(cli.get_help())
