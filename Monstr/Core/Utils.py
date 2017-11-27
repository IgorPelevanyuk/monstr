import datetime
import pytz
import urllib2
import ssl

def get_page(url):
    """Return html code from cmsweb.cern.ch of the page with the given url"""
    print "HTTP access: ", url
    try:
        socket_obj = urllib2.urlopen(url)
    except Exception, e:
        socket_obj = urllib2.urlopen(url, context=ssl._create_unverified_context())
    page = socket_obj.read()
    socket_obj.close()
    return page

def epoch_to_datetime(seconds):
    new_time = datetime.datetime.utcfromtimestamp(float(seconds)).replace(tzinfo=pytz.utc)
    return new_time

def datetime_to_epoch(timestamp):
    """Convert datetime timestamp to epoch seconds(double). Miliseconds comes as decimal.
    Return example:
    >>> datetime_to_epoch(x)
    1479730200.0"""
    epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
    result = (timestamp - epoch).total_seconds()
    return result

def get_UTC_now():
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

def build_URL(baseURL, params):
    URL = baseURL
    for param in params:
        URL = URL.replace('<' + param + '>', str(params[param]))
    return URL
