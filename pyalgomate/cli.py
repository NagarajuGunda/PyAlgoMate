
import click
import zmq
import json
import logging
import datetime
import pyalgomate.utils as utils

# ZeroMQ Context
context = zmq.Context()


@click.group()
@click.pass_context
def cli(ctx):
    global strategyClass
    ctx.obj = strategyClass


@cli.command(name='backtest')
@click.option('--underlying', default='BANKNIFTY', help='Specify an underlying')
@click.option('--data', prompt='Specify data file', multiple=True)
@click.pass_obj
def runBacktest(strategyClass, underlying, data):
    from pyalgomate.backtesting import CustomCSVFeed
    from pyalgomate.brokers import BacktestingBroker

    start = datetime.datetime.now()
    feed = CustomCSVFeed.CustomCSVFeed()
    feed.addBarsFromParquets(dataFiles=data, ticker=underlying)

    print("")
    print(f"Time took in loading data <{datetime.datetime.now()-start}>")
    start = datetime.datetime.now()

    broker = BacktestingBroker(200000, feed)
    strat = strategyClass(feed=feed, broker=broker, underlying=underlying, lotSize=25)

    strat.run()

    print("")
    print(
        f"Time took in running the strategy <{datetime.datetime.now()-start}>")


@cli.command(name='trade')
@click.option('--broker', prompt='Select a broker', type=click.Choice(['Finvasia', 'Zerodha']), help='Select a broker')
@click.option('--mode', prompt='Select a trading mode', type=click.Choice(['paper', 'live']), help='Select a trading mode')
@click.option('--underlying', help='Specify an underlying')
@click.pass_obj
def runLiveTrade(strategyClass, broker, mode, underlying):
    if not broker:
        raise click.UsageError('Please select a broker')

    if not mode:
        raise click.UsageError('Please select a mode')

    import yaml
    import pyotp
    import datetime
    import os

    # Define the socket using the "Context"
    sock = context.socket(zmq.PUB)
    sock.bind("tcp://127.0.0.1:5680")

    def valueChangedCallback(strategy, value):
        jsonDump = json.dumps({strategy: value})
        logger.debug(jsonDump)
        sock.send_json(jsonDump)

    logger = logging.getLogger(__file__)

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
            logger.info(f"Token has been created today already. Re-using it")
            with open(tokenFile, 'r') as f:
                userToken = f.read()
            logger.info(
                f"userid {cred['user']} password ******** usertoken {userToken}")
            loginStatus = api.set_session(
                userid=cred['user'], password=cred['pwd'], usertoken=userToken)
        else:
            print(f"Logging in and persisting user token")
            loginStatus = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

            with open(tokenFile, 'w') as f:
                f.write(loginStatus.get('susertoken'))

            logger.info(
                f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")

        if loginStatus != None:
            underlyingInstrument = 'NSE|NIFTY BANK' if underlying is None else underlying

            ltp = api.get_quotes('NSE', getFinvasiaToken(
                api, underlyingInstrument))['lp']

            currentWeeklyExpiry = utils.getNearestWeeklyExpiryDate(
                datetime.datetime.now().date())
            monthlyExpiry = utils.getNearestMonthlyExpiryDate(
                datetime.datetime.now().date())
            monthlyExpiry = utils.getNextMonthlyExpiryDate(
                datetime.datetime.now().date()) if monthlyExpiry == currentWeeklyExpiry else monthlyExpiry

            optionSymbols = finvasia.broker.getOptionSymbols(
                underlyingInstrument, currentWeeklyExpiry, ltp, 10)

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
        monthlyExpiry = utils.getNearestMonthlyExpiryDate(
            datetime.datetime.now().date())
        monthlyExpiry = utils.getNextMonthlyExpiryDate(
            datetime.datetime.now().date()) if monthlyExpiry == currentWeeklyExpiry else monthlyExpiry

        optionSymbols = zerodha.broker.getOptionSymbols(
            underlyingInstrument, currentWeeklyExpiry, ltp, 10)

        optionSymbols = list(dict.fromkeys(optionSymbols))

        tokenMappings = getZerodhaTokensList(
            api, [underlyingInstrument] + optionSymbols)

        barFeed = ZerodhaLiveFeed(api, tokenMappings)
        if mode == 'paper':
            broker = ZerodhaPaperTradingBroker(200000, barFeed)
        else:
            broker = ZerodhaLiveBroker(api)

    strategy = strategyClass(feed=barFeed, broker=broker, underlying=underlyingInstrument,
                       registeredOptionsCount=len(optionSymbols), lotSize=25,
                       callback=valueChangedCallback, collectData=True)

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
