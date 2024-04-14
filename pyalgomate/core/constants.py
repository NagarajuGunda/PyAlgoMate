"""defining constants"""
import pytz

API_SCRIP_MASTER_FILE = "api-scrip-master.csv"

utc_tz = pytz.timezone('UTC')
ist_tz = pytz.timezone('Asia/Kolkata')

DATE_FORMAT = '%Y-%m-%d'
DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_TIME_FORMAT_WITHOUT_SEC = '%Y-%m-%d %H:%M:00'
