#!/bin/python
from datetime import timedelta
import json
from sqlalchemy.sql import func

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule
from Monstr.Core.DB import Column, BigInteger, Integer, String, DateTime

import pytz




class PhedexTransfers(BaseModule.BaseModule):
    name = 'PhedexTransfers'
    #table_schemas = {'main': (Column('id', Integer, primary_key=True),
    table_schemas = {'main': (Column('instance', String(10), primary_key=True),
                              Column('to', String(40), primary_key=True),
                              Column('from', String(40), primary_key=True),
                              Column('done_files', Integer),
                              Column('done_bytes', BigInteger),
                              Column('try_files', Integer),
                              Column('try_bytes', BigInteger),
                              Column('expire_files', Integer),
                              Column('expire_bytes', BigInteger),
                              Column('fail_files', Integer),
                              Column('fail_bytes', BigInteger),
                              Column('time', DateTime(True), primary_key=True),
                              Column('binwidth', Integer))
                             #rate: done_bytes/binwidth(3600)
                    }

    config = {'site': 'T1_RU_JINR',
              'binwidth': 600}

    HOSTNAME = "http://cmsweb.cern.ch"
    REQUESTS = {"prod": {"from": "/phedex/datasvc/json/prod/TransferHistory?from=<site>&binwidth=<binwidth>&endtime=<endtime>",
                         "to": "/phedex/datasvc/json/prod/TransferHistory?to=<site>&binwidth=<binwidth>&endtime=<endtime>"},
                "debug": {"from": "/phedex/datasvc/json/debug/TransferHistory?from=<site>&binwidth=<binwidth>&endtime=<endtime>",
                          "to": "/phedex/datasvc/json/debug/TransferHistory?to=<site>&binwidth=<binwidth>&endtime=<endtime>"}}

    def __init__(self):
        super(PhedexTransfers, self).__init__()
        self.db_handler = DB.DBHandler()
        self.rest_links['lastStatus'] = self.lastStatus

    def PrepareRetrieve(self):
        # TODO: Introduce filling of absent hours
        current_time = Utils.get_UTC_now().replace(second=0, microsecond=0)
        last_row = self.db_handler.get_session().query(func.max(self.tables['main'].c.time).label("max_time_done")).one()
        print last_row
        horizon = Utils.get_UTC_now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
        if last_row[0]:
            last_time = last_row[0].astimezone(pytz.utc)
            if current_time - last_time < timedelta(hours=2):
                horizon = last_time + timedelta(seconds=self.config['binwidth'])

        return {'horizon': horizon}

    def Retrieve(self, params):
        insert_list = []
        retrieve_time = Utils.get_UTC_now()
        horizon = params['horizon']
        horizon = Utils.get_UTC_now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
        binwidth = self.config['binwidth']
        endtime = horizon + timedelta(seconds=binwidth)
        cross_data = []
        while endtime < retrieve_time:
            for instance in self.REQUESTS:
                for direction in self.REQUESTS[instance]:
                    params = {}
                    params['site'] = self.config['site'] + '*'
                    params['binwidth'] = binwidth
                    params['endtime'] = Utils.datetime_to_epoch(endtime)
                    url = Utils.build_URL(self.HOSTNAME + self.REQUESTS[instance][direction], params)
                    json_raw = Utils.get_page(url)
                    json_obj = json.loads(json_raw)['phedex']['link']
                    for link in json_obj:
                        details = link['transfer'][0]
                        transfer = {'instance': instance,
                                    'from': link['from'],
                                    'to': link['to'],
                                    'done_files': details['done_files'],
                                    'done_bytes': details['done_bytes'],
                                    'try_files': details['try_files'],
                                    'try_bytes': details['try_bytes'],
                                    'expire_files': details['expire_files'],
                                    'expire_bytes': details['expire_bytes'],
                                    'fail_files': details['fail_files'],
                                    'fail_bytes': details['fail_bytes'],
                                    'time': endtime - timedelta(seconds=binwidth),
                                    'binwidth': binwidth,
                                   }
                        if self.config['site'] in link['to'] and self.config['site'] in link['from']:
                            cross_data.append(transfer)
                        else:
                            insert_list.append(transfer)
            endtime += timedelta(seconds=binwidth)
        cross_data = [dict(t) for t in set([tuple(d.items()) for d in cross_data])]
        insert_list += cross_data
        return {'main': insert_list}

    def InsertToDB(self, data):
        for schema in data:
            table = self.tables[schema]
            min_time = min([x['time'] for x in data[schema]])
            print min_time
            print len(data[schema])
            d = table.delete(table.c.time >= min_time)
            d.execute()
            self.db_handler.bulk_insert(table, data[schema])

    #==========================================================================
    #                 Web
    #==========================================================================

    def lastStatus(self, incoming_params):
        response = {}
        try:
            default_params = {'delta': 1, 'instance': 'prod'}
            params = self._create_params(default_params, incoming_params)
            result = []
            max_time = self.db_handler.get_session().query(func.max(self.tables['main'].c.time).label("max_time")).one()
            if max_time[0]:
                max_time = max_time[0]
                query = self.tables['main'].select((self.tables['main'].c.time > max_time - timedelta(hours=params['delta']))&(self.tables['main'].c.instance == params['instance']))
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


def main():
    X = PhedexTransfers()
    #pp(X.Retrieve({'horizon': Utils.get_UTC_now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)}))
    X.ExecuteCheck()

if __name__ == '__main__':
    main()
