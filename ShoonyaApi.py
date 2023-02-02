import yaml
import concurrent.futures
import time
from NorenRestApiPy.NorenApi import NorenApi
import logging
import pyotp

# enable dbug to see request and responses
logging.basicConfig(level=logging.DEBUG)

api = None

class Order:
    def __init__(self, buy_or_sell: str = None, product_type: str = None,
                 exchange: str = None, tradingsymbol: str = None,
                 price_type: str = None, quantity: int = None,
                 price: float = None, trigger_price: float = None, discloseqty: int = 0,
                 retention: str = 'DAY', remarks: str = "tag",
                 order_id: str = None):
        self.buy_or_sell = buy_or_sell
        self.product_type = product_type
        self.exchange = exchange
        self.tradingsymbol = tradingsymbol
        self.quantity = quantity
        self.discloseqty = discloseqty
        self.price_type = price_type
        self.price = price
        self.trigger_price = trigger_price
        self.retention = retention
        self.remarks = remarks
        self.order_id = None

    # print(ret)


def get_time(time_string):
    data = time.strptime(time_string, '%d-%m-%Y %H:%M:%S')

    return time.mktime(data)

#flag to tell us if the websocket is open
socket_opened = False

#application callbacks
def event_handler_order_update(message):
    print("order event: " + str(message))


def event_handler_quote_update(message):
    #e   Exchange
    #tk  Token
    #lp  LTP
    #pc  Percentage change
    #v   volume
    #o   Open price
    #h   High price
    #l   Low price
    #c   Close price
    #ap  Average trade price

    print("quote event: " + str(message))
    

def open_callback():
    global socket_opened
    socket_opened = True
    print('app is connected')
    #api.subscribe_orders()
    api.subscribe('NSE|22')
    #api.subscribe(['NSE|22', 'BSE|522032'])

#end of callbacks

class ShoonyaApi(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')
        global api
        api = self

    def place_basket(self, orders):

        resp_err = 0
        resp_ok = 0
        result = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

            future_to_url = {executor.submit(
                self.place_order, order): order for order in orders}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
            try:
                result.append(future.result())
            except Exception as exc:
                print(exc)
                resp_err = resp_err + 1
            else:
                resp_ok = resp_ok + 1

        return result

    def placeOrder(self, order: Order):
        ret = NorenApi.place_order(self, buy_or_sell=order.buy_or_sell, product_type=order.product_type,
                                   exchange=order.exchange, tradingsymbol=order.tradingsymbol,
                                   quantity=order.quantity, discloseqty=order.discloseqty, price_type=order.price_type,
                                   price=order.price, trigger_price=order.trigger_price,
                                   retention=order.retention, remarks=order.remarks)
        # print(ret)

        return ret


api = ShoonyaApi()

with open('cred.yml') as f:
    cred = yaml.load(f, Loader=yaml.FullLoader)
    print(cred)

ret = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

print(ret)

if ret != None:   
    while True:
        print('p => place order')
        print('m => modify order')
        print('c => cancel order')
        print('y => order history')
        print('o => get order book')
        print('h => get holdings')
        print('l => get limits')
        print('k => get positions')
        print('d => get daily mtm')
        print('s => start_websocket')
        print('q => quit')

        prompt1=input('what shall we do? ').lower()        
            
        if prompt1 == 'p':
            ret = api.place_order(buy_or_sell='B', product_type='C',
                        exchange='NSE', tradingsymbol='INFY-EQ', 
                        quantity=1, discloseqty=0,price_type='LMT', price=1500.00, trigger_price=None,
                        retention='DAY', remarks='my_order_001')
            print(ret)

        elif prompt1 == 'm':
            orderno=input('Enter orderno:').lower()        
            ret = api.modify_order(exchange='NSE', tradingsymbol='INFY-EQ', orderno=orderno,
                                   newquantity=2, newprice_type='LMT', newprice=1505.00)
            print(ret)

        elif prompt1 == 'c':
            orderno=input('Enter orderno:').lower()        
            ret = api.cancel_order(orderno=orderno)
            print(ret)

        elif prompt1 == 'y':
            orderno=input('Enter orderno:').lower()        
            ret = api.single_order_history(orderno=orderno)
            print(ret)
            
        elif prompt1 == 'o':            
            ret = api.get_order_book()
            print(ret)

        elif prompt1 == 'h':            
            ret = api.get_holdings()
            print(ret)

        elif prompt1 == 'l':            
            ret = api.get_limits()
            print(ret)

        elif prompt1 == 'k':            
            ret = api.get_positions()
            print(ret)
        elif prompt1 == 'd':            
            #contributed by Aromal P Nair
            while True:
                ret = api.get_positions()
                mtm = 0
                pnl = 0
                for i in ret:
                    mtm += float(i['urmtom'])
                    pnl += float(i['rpnl'])
                    day_m2m = mtm + pnl
                print(day_m2m)
        elif prompt1 == 's':
            if socket_opened == True:
                print('websocket already opened')
                continue
            ret = api.start_websocket(order_update_callback=event_handler_order_update, subscribe_callback=event_handler_quote_update, socket_open_callback=open_callback)
            print(ret)
        else:
            print('Fin') #an answer that wouldn't be yes or no
            break
