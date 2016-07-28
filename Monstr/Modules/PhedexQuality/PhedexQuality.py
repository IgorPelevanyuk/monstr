#!/bin/python

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule

from datetime import timedelta
import json
import pytz

from Monstr.Core.DB import Column, Integer, String, DateTime, BigInteger
from sqlalchemy.sql import func

class PhedexQuality(BaseModule.BaseModule):
    name = 'PhedexQuality'
    table_schemas = {'main': (Column('id', Integer, primary_key=True),
                              Column('instance', String(10)),
                              Column('time', DateTime(True)),
                              Column('site', String(60)),
                              Column('rate', Integer),
                              Column('quality', String(10)),
                              Column('done_files', Integer),
                              Column('done_bytes', BigInteger),
                              Column('try_bytes', BigInteger),
                              Column('fail_files', Integer),
                              Column('fail_bytes', BigInteger),
                              Column('expire_files', Integer),
                              Column('expire_bytes', BigInteger),)
                    }

    HOSTNAME = "http://cmsweb.cern.ch"
    REQUESTS = {'prod': '/phedex/datasvc/json/prod/transferhistory?starttime=-168h&to=T1_RU_JINR*',
                'debug': '/phedex/datasvc/json/debug/transferhistory?starttime=-168h&to=T1_RU_JINR*'}

    
    config = {}
    default_config = {'period': 1}

    def __init__(self, config=None):
        super(PhedexQuality, self).__init__()
        self.db_handler = DB.DBHandler()
        self.config = self.default_config
        if config is not None:
            self.config.update(config)

    def refactorQuality(self, quality):
        result = {}
        for link in quality:
            site = str(link['from'])
            result[site] = {}
            for transfer in link['transfer']:
                result[site][str(transfer['timebin'])] = transfer
        return result

    def Retrieve(self, params):

        result = []
        #Get current time and last recorded time
        current_time = Utils.get_UTC_now().replace(minute=0, second=0, microsecond=0)
        last_time = None
        last_row = self.db_handler.get_session().query(func.max(self.tables['main'].c.time).label("max_time")).one()
        if last_row[0]:
            last_time = last_row[0].astimezone(pytz.utc) + timedelta(hours=1)
            if current_time - last_row[0] > timedelta(hours=self.config['period']):
                last_time = current_time - timedelta(hours=self.config['period'])
        else:
            last_time = current_time - timedelta(hours=self.config['period'])

        # Gather all data hour by hour
        while last_time < current_time:

            for instance in self.REQUESTS:
                quality_json = Utils.get_page(self.HOSTNAME + self.REQUESTS[instance])
                quality = json.loads(quality_json)['phedex']['link']            
                quality = self.refactorQuality(quality)
                for site in quality:
                    for time in quality[site]:
                        if not quality[site][time]['quality']:
                            continue
                        result.append({'instance': str(instance),
                                       'site': str(site), 
                                       'time': Utils.epoch_to_datetime(time),
                                       'rate': int(quality[site][time]['rate']),
                                       'quality': float(quality[site][time]['quality']), 
                                       'done_files': int(quality[site][time]['done_files']), 
                                       'done_bytes': int(quality[site][time]['done_bytes']), 
                                       'try_files': int(quality[site][time]['try_files']),
                                       'try_bytes': int(quality[site][time]['try_bytes']),
                                       'fail_files': int(quality[site][time]['fail_files']),
                                       'fail_bytes': int(quality[site][time]['fail_bytes']),
                                       'expire_files':int(quality[site][time]['expire_files']),
                                       'expire_bytes':int(quality[site][time]['expire_bytes']),
                                       })

            last_time = last_time + timedelta(hours=1)

        return {'main': result}

if __name__ == '__main__':
    X = PhedexQuality()
    X.ExecuteCheck()
