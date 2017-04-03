#!/bin/python
from datetime import timedelta
import json
from sqlalchemy.sql import func

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule
from Monstr.Core.DB import Column, Integer, String, DateTime, BigInteger

import pytz

class PhedexQuality(BaseModule.BaseModule):
    name = 'PhedexQuality'
    table_schemas = {'main': (Column('id', Integer, primary_key=True),
                              Column('instance', String(10)),
                              Column('direction', String(4)),
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
    REQUEST = "/phedex/datasvc/json/<instance>/transferhistory?starttime=-168h&<direction>=T1_RU_JINR*"

    config = {}
    default_config = {'period': 1}

    def __init__(self, config=None):
        super(PhedexQuality, self).__init__()
        self.db_handler = DB.DBHandler()
        self.config = self.default_config
        if config is not None:
            self.config.update(config)

    def refactorQuality(self, quality, direction):
        result = {}
        if direction == 'to':
            direction = 'from'
        else:
            direction = 'to'
        for link in quality:
            site = str(link[direction])
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

            for instance in ['prod', 'debug']:
                for direction in ['from', 'to']:
                    quality_url = Utils.build_URL(self.HOSTNAME + self.REQUEST, {'instance': instance, 'direction': direction})
                    quality_json = Utils.get_page(quality_url)
                    quality = json.loads(quality_json)['phedex']['link']
                    quality = self.refactorQuality(quality, direction)
                    for site in quality:
                        for time in quality[site]:
                            if not quality[site][time]['quality']:
                                continue
                            result.append({'instance': str(instance),
                                           'site': str(site),
                                           'direction': str(direction),
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

    #==========================================================================
    #                 Web
    #==========================================================================

    def lastStatus(self, incoming_params):
        response = {}
        try:
            default_params = {'delta': 8, 'instance': 'prod', 'direction': 'to'}
            params = self._create_params(default_params, incoming_params)
            result = []
            max_time = self.db_handler.get_session().query(func.max(self.tables['main'].c.time).label("max_time")).one()
            if max_time[0]:
                max_time = max_time[0]
                query = self.tables['main'].select((self.tables['main'].c.time > max_time - timedelta(hours=params['delta']))&(self.tables['main'].c.instance == params['instance'])&(self.tables['main'].c.direction == params['direction']))
                cursor = query.execute()
                resultProxy = cursor.fetchall()
                for row in resultProxy:
                    result.append(dict(row.items()))
            response = {'data': result,
                        'applied_params': params,
                        'success': True}
        except Exception as e:
            response = {'data': result,
                        'incoming_params': incoming_params,
                        'default_params': [[key, default_params[key], type(default_params[key])] for key in default_params],
                        'success': False,
                        'error': type(e).__name__ + ': ' + e.message}

        return response

    rest_links = {'lastStatus': lastStatus}

def main():
    X = PhedexQuality()
    X.ExecuteCheck()

if __name__ == '__main__':
    main()
