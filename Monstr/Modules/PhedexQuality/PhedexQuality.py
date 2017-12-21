#!/bin/python
from datetime import timedelta
import json
from pprint import pprint as pp
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
                              Column('rate', BigInteger),
                              Column('quality', String(10)),
                              Column('done_files', Integer),
                              Column('done_bytes', BigInteger),
                              Column('try_bytes', BigInteger),
                              Column('fail_files', Integer),
                              Column('fail_bytes', BigInteger),
                              Column('expire_files', Integer),
                              Column('expire_bytes', BigInteger))}

    status_list = [
                   # Debug To
                   {'name': 'DebugToAll', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugToT0', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugToT1', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugToJINR', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugToOther', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   # Debug From
                   {'name': 'DebugFromAll', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugFromT0', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugFromT1', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugFromJINR', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'DebugFromOther', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   # Prod To
                   {'name': 'ProdToAll', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdToT0', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdToT1', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdToJINR', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdToOther', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   # Prod From
                   {'name': 'ProdFromAll', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdFromT0', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdFromT1', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdFromJINR', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'ProdFromOther', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   ]

    HOSTNAME = "http://cmsweb.cern.ch"
    REQUEST = "/phedex/datasvc/json/<instance>/transferhistory?starttime=-168h&<direction>=T1_RU_JINR*"

    config = {}
    default_config = {'period': 1}

    def __init__(self, config=None):
        super(PhedexQuality, self).__init__()
        self.db_handler = DB.DBHandler()
        self.rest_links['lastStatus'] = self.lastStatus
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
        # Get current time and last recorded time
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
                                           'expire_files': int(quality[site][time]['expire_files']),
                                           'expire_bytes': int(quality[site][time]['expire_bytes'])})
                            if int(quality[site][time]['rate']) > 2147483647:
                                pp('rate ' + str(quality[site][time]['rate']))
                            if int(quality[site][time]['done_files']) > 2147483647:
                                pp('done_files ' + str(quality[site][time]['done_files']))
                            if int(quality[site][time]['try_files']) > 2147483647:
                                pp('try_files ' + str(quality[site][time]['try_files']))
                            if int(quality[site][time]['fail_files']) > 2147483647:
                                pp('fail_files ' + str(quality[site][time]['fail_files']))
                            if int(quality[site][time]['expire_files']) > 2147483647:
                                pp('expire_files ' + str(quality[site][time]['expire_files']))

            last_time = last_time + timedelta(hours=1)
        return {'main': result}

    # --------------------------------------------------------------------------
    # Analyzis
    # --------------------------------------------------------------------------
    def _get_quality_status(self, quality):
        return {quality < 0.20: 50,
                0.2 <= quality < 0.6: 40,
                0.6 <= quality < 0.8: 30,
                0.8 <= quality < 0.9: 20,
                0.9 <= quality: 10}[True]

    def _get_data_from_db(self, instance, direction, site_substr, period=8):
        max_time = self.db_handler.get_session().query(func.max(self.tables['main'].c.time).label("max_time")).one()
        if max_time[0]:
            max_time = max_time[0]
            query = 0
            query = self.tables['main'].select((self.tables['main'].c.time > max_time - timedelta(hours=period)) &
                                               (self.tables['main'].c.instance == instance) &
                                               (self.tables['main'].c.direction == direction) &
                                               (self.tables['main'].c.site.contains(site_substr)))
            cursor = query.execute()
            resultProxy = cursor.fetchall()
            links = {}
            for row in resultProxy:
                link = dict(row.items())
                if link['site'] in links:
                    links[link['site']][link['time']] = link
                else:
                    links[link['site']] = {link['time']: link}
        return links

    def Analyze(self, data):
        new_statuses = []
        translation = {'T2': 'Other',
                       '_': 'All'}
        update_time = Utils.get_UTC_now()
        for instance in ['debug', 'prod']:
            for direction in ['from', 'to']:
                for site_substr in ['T0', 'T1', 'JINR', 'T2', '_']:
                    data_selection = self._get_data_from_db(instance, direction, site_substr)
                    qualities = [float(data_selection[site][time]['quality']) for site in data_selection for time in data_selection[site]]
                    if len(qualities) != 0:
                        quality = sum(qualities)/len(qualities)
                        site_name = translation[site_substr] if site_substr in translation else site_substr

                        status_name = instance.capitalize() + direction.capitalize() + site_name
                        new_statuses.append({'name': status_name, 'status': self._get_quality_status(quality), 'time': update_time, 'description': 'Quality is ' + str(quality)})

        print new_statuses
        self.update_status(new_statuses)

    # ==========================================================================
    #                 Web
    # ==========================================================================

    def lastStatus(self, incoming_params):
        response = {}
        try:
            default_params = {'delta': 8, 'instance': 'prod', 'direction': 'to'}
            params = self._create_params(default_params, incoming_params)
            result = []
            max_time = self.db_handler.get_session().query(func.max(self.tables['main'].c.time).label("max_time")).one()
            if max_time[0]:
                max_time = max_time[0]
                query = self.tables['main'].select((self.tables['main'].c.time > max_time - timedelta(hours=params['delta'])) & (self.tables['main'].c.instance == params['instance'])&(self.tables['main'].c.direction == params['direction']))
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
    X = PhedexQuality()
    X.ExecuteCheck()

if __name__ == '__main__':
    main()
