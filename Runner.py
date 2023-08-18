import os
import datetime
import yaml
from importlib import import_module
import threading
import logging
import signal
import pyalgomate.utils as utils
from pyalgomate.telegram import TelegramBot

logging.basicConfig(filename=f'PyAlgoMate.log', level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)

logger = logging.getLogger()


def getFeed(creds, broker, registerOptions=['Weekly'], underlyings=['NSE|NIFTY BANK']):
    if broker == 'Finvasia':
        from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi
        from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker, getFinvasiaToken, getFinvasiaTokenMappings
        import pyalgomate.brokers.finvasia as finvasia
        from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
        import pyotp

        cred = creds[broker]

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
            logger.info(f"Logging in and persisting user token")
            loginStatus = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

            if loginStatus:
                with open(tokenFile, 'w') as f:
                    f.write(loginStatus.get('susertoken'))

                logger.info(
                    f"{loginStatus.get('uname')}={loginStatus.get('stat')} token={loginStatus.get('susertoken')}")
            else:
                logger.info(f'Login failed!')

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

                if "Weekly" in registerOptions:
                    optionSymbols += finvasia.broker.getOptionSymbols(
                        underlying, currentWeeklyExpiry, ltp, 10)
                if "NextWeekly" in registerOptions:
                    optionSymbols += finvasia.broker.getOptionSymbols(
                        underlying, nextWeekExpiry, ltp, 10)
                if "Monthly" in registerOptions:
                    optionSymbols += finvasia.broker.getOptionSymbols(
                        underlying, monthlyExpiry, ltp, 10)

            optionSymbols = list(dict.fromkeys(optionSymbols))

            tokenMappings = getFinvasiaTokenMappings(
                api, underlyings + optionSymbols)

            barFeed = LiveTradeFeed(api, tokenMappings)
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

            if "Weekly" in registerOptions:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, currentWeeklyExpiry, ltp, 10)
            if "NextWeekly" in registerOptions:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, nextWeekExpiry, ltp, 10)
            if "Monthly" in registerOptions:
                optionSymbols += zerodha.broker.getOptionSymbols(
                    underlying, monthlyExpiry, ltp, 10)

        optionSymbols = list(dict.fromkeys(optionSymbols))

        tokenMappings = getZerodhaTokensList(
            api, underlyings + optionSymbols)

        barFeed = ZerodhaLiveFeed(api, tokenMappings)

    return barFeed, api


def getBroker(feed, api, broker, mode, capital=200000):
    if broker == 'Finvasia':
        from pyalgomate.brokers.finvasia.broker import PaperTradingBroker, LiveBroker

        if mode == 'paper':
            brokerInstance = PaperTradingBroker(200000, feed)
        else:
            brokerInstance = LiveBroker(api)
    elif broker == 'Zerodha':
        from pyalgomate.brokers.zerodha.broker import ZerodhaPaperTradingBroker, ZerodhaLiveBroker

        if mode == 'paper':
            brokerInstance = ZerodhaPaperTradingBroker(capital, feed)
        else:
            brokerInstance = ZerodhaLiveBroker(api)

    return brokerInstance


def main():
    with open("strategies.yaml", "r") as file:
        config = yaml.safe_load(file)

    with open('cred.yml') as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    if 'Telegram' in creds and 'token' in creds['Telegram']:
        telegramBot = TelegramBot(
            creds['Telegram']['token'], creds['Telegram']['chatid'])

    strategies = []

    feed, api = getFeed(creds, config['Broker'])

    for strategyName, details in config['Strategies'].items():
        strategyClassName = details['Class']
        strategyPath = details['Path']
        strategyMode = details['Mode']
        strategyArgs = details['Args']
        strategyArgs.append({'telegramBot': telegramBot})
        strategyArgs.append({'strategyName': strategyName})

        module = import_module(
            strategyPath.replace('.py', '').replace('/', '.'))
        strategyClass = getattr(module, strategyClassName)
        strategyArgsDict = {
            key: value for item in strategyArgs for key, value in item.items()}

        broker = getBroker(feed, api, config['Broker'], strategyMode)

        strategyInstance = strategyClass(
            feed=feed, broker=broker, **strategyArgsDict)

        strategies.append(strategyInstance)

    threads = []

    for strategyObject in strategies:
        thread = threading.Thread(target=strategyObject.run)
        thread.start()
        threads.append(thread)

    if telegramBot:
        def handle_interrupt(signum, frame):
            logger.info("Ctrl+C received. Stopping the bot...")
            telegramBot.stop()
            telegramBot.waitUntilFinished()
            telegramBot.delete()
            logger.info("Bot stopped. Exiting the process.")
            exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
