import logging
import os
import sentry_sdk
from dotenv import load_dotenv

logger = logging.getLogger()
logger.setLevel(logging.INFO)
load_dotenv()

# Sentry error logging setup
sentry_dns = os.getenv('SENTRY_DNS', None)
if sentry_dns and os.getenv('LOCAL_ENV', 'FALSE') != 'TRUE':
    sentry_sdk.init(sentry_dns, server_name=os.getenv('PROD_PROVIDER', 'Trident'))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fileHandler = logging.FileHandler('PyAlgoMate.log')
fileHandler.setLevel(logging.INFO)
fileHandler.setFormatter(formatter)

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
consoleHandler.setFormatter(formatter)

logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)

logging.getLogger("requests").setLevel(logging.WARNING)
