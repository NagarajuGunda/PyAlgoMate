import yaml
import pyotp
import logging

from pyalgomate.brokers.finvasia.feed import LiveTradeFeed
from pyalgomate.brokers.finvasia.broker import PaperTradingBroker
from pyalgomate.strategies.Strategy import Strategy

from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApi

logging.basicConfig(level=logging.INFO)


def main():
    with open('cred.yml') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)

    api = ShoonyaApi(host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')

    ret = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                    vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

    if ret != None:
        barFeed = LiveTradeFeed(api, ['NSE|26009', 'NSE|26000'])
        broker = PaperTradingBroker(200000, barFeed)
        strat = Strategy(barFeed, broker)

    strat.run()


if __name__ == "__main__":
    main()
