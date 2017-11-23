#!/bin/python

from datetime import timedelta
from pprint import pprint as pp
import json

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule

import pytz

from Monstr.Core.DB import Column, Integer, String, DateTime, UniqueConstraint, func


class CMSJobStatus(BaseModule.BaseModule):
    name = 'CMSJobStatus'
    table_schemas = {'main': (Column('id', Integer, primary_key=True),
                              Column('time', DateTime(True)),
                              Column('site_name', String(60)),
                              Column('aborted', Integer),
                              Column('app_succeeded', Integer),
                              Column('applic_failed', Integer),
                              Column('application_failed', Integer),
                              Column('cancelled', Integer),
                              Column('pending', Integer),
                              Column('running', Integer),
                              Column('site_failed', Integer),
                              Column('submitted', Integer),
                              UniqueConstraint("time", "site_name"),)}

    status_list = [{'name': 'load', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'rank', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'site_failures', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''}]

    # tables = None
    config = {}
    default_config = {'period': 8}

    def __init__(self, config=None):
        super(CMSJobStatus, self).__init__()
        self.db_handler = DB.DBHandler()
        self.rest_links['lastStatus'] = self.lastStatus
        self.config = self.default_config
        if config is not None:
            self.config.update(config)

    def isInteresting(self, site_name):
        if site_name.startswith('T1'):
            return True
        if site_name.startswith('T0'):
            return True
        if site_name == 'T2_CH_CERN':
            return True
        return False

    def Retrieve(self, params):
        # Get current time and last recorded time
        current_time = Utils.get_UTC_now().replace(minute=0, second=0, microsecond=0)
        last_time = current_time - timedelta(hours=self.config['period'])
        # Gather all data hour by hour
        update_incert_list = []
        while last_time < current_time:
            begin = last_time
            end = last_time + timedelta(hours=1)
            time1 = '+' + str(begin.hour) + "%3A00"
            time2 = '+' + str(end.hour) + "%3A00"
            date1 = str(begin).split(' ')[0] + time1
            date2 = str(end).split(' ')[0] + time2
            url = "http://dashb-cms-job.cern.ch/dashboard/request.py/jobsummary-plot-or-table2?user=&submissiontool=&application=&activity=&status=&check=terminated&tier=&sortby=site&ce=&rb=&grid=&jobtype=&submissionui=&dataset=&submissiontype=&task=&subtoolver=&genactivity=&outputse=&appexitcode=&accesstype=&inputse=&cores=&date1=" + date1 + "&date2=" + date2 + "&prettyprint"

            json_raw = Utils.get_page(url)
            json_obj = json.loads(json_raw)['summaries']
            for obj in json_obj:
                site_name = str(obj['name'])
                if self.isInteresting(site_name):
                    current_status = {'site_name': site_name,
                                      'time': last_time,
                                      'applic_failed': int(obj['applic-failed']),
                                      'app_succeeded': int(obj['app-succeeded']),
                                      'pending': int(obj['pending']),
                                      'running': int(obj['running']),
                                      'aborted': int(obj['aborted']),
                                      'application_failed': int(obj['application-failed']),
                                      'site_failed': int(obj['site-failed']),
                                      'cancelled': int(obj['cancelled']),
                                      'submitted': int(obj['submitted'])}

                    update_incert_list.append(current_status)
            last_time = last_time + timedelta(hours=1)
        return {'main': update_incert_list}

    def InsertToDB(self, data):
        for schema in data:
            table = self.tables[schema]
            min_time = min([x['time'] for x in data[schema]])
            d = table.delete(table.c.time >= min_time)
            d.execute()
            self.db_handler.bulk_insert(table, data[schema])

    # --------------------------------------------------------------------------
    # Helper functions for Analyze
    # --------------------------------------------------------------------------

    def _get_sites_success_fail(self, check_time, interval=8):
        from sqlalchemy.sql import select
        result = {}
        query = select([self.tables['main'].c.site_name,
                        func.sum(self.tables['main'].c.app_succeeded).label('app_succeeded'),
                        func.sum(self.tables['main'].c.site_failed).label('site_failed')])\
            .where(self.tables['main'].c.time > check_time - timedelta(hours=interval))\
            .group_by(self.tables['main'].c.site_name)
        cursor = query.execute()
        resultProxy = cursor.fetchall()
        for row in resultProxy:
            item = dict(row.items())
            result[item['site_name']] = {'app_succeeded': item['app_succeeded'],
                                         'site_failed': item['site_failed'],
                                         'site_name': item['site_name']}
        return result

    def _get_fail_ratio_status(self, fail_ratio):
        return {fail_ratio < 0.01: 10,
                0.01 <= fail_ratio < 0.05: 20,
                0.05 <= fail_ratio < 0.12: 30,
                0.12 <= fail_ratio < 0.3: 40,
                0.3 <= fail_ratio: 50}[True]

    def _get_load_status(self, load):
        return {load > 25000: 10,
                25000 >= load > 15000: 20,
                15000 >= load > 7000: 30,
                7000 >= load > 1000: 40,
                1000 >= load: 50}[True]

    def _get_rank_status(self, rank):
        return {rank < 2: 10,
                2 <= rank < 3: 20,
                3 <= rank < 4: 30,
                4 <= rank < 6: 40,
                6 <= rank: 50}[True]

    def Analyze(self, data):
        new_statuses = []
        update_time = Utils.get_UTC_now()

        site_info = self._get_sites_success_fail(update_time)
        app_succeeded = site_info['T1_RU_JINR']['app_succeeded']
        site_failed = site_info['T1_RU_JINR']['site_failed']

        fail_ratio = 1.0 * site_failed / (site_failed + app_succeeded)
        load = site_failed + app_succeeded

        sorted_list = list(reversed(sorted([site_info[site]for site in site_info], key=lambda x: x['app_succeeded'])))
        print sorted_list
        for i in range(0, len(sorted_list)):
            if sorted_list[i]['site_name'] == 'T1_RU_JINR':
                rank = i

        new_statuses.append({'name': 'load', 'status': self._get_load_status(load), 'time': update_time, 'description': 'Load: is ' + str(load)})
        new_statuses.append({'name': 'rank', 'status': self._get_rank_status(rank), 'time': update_time, 'description': 'Rank: is ' + str(rank)})
        new_statuses.append({'name': 'site_failures', 'status': self._get_fail_ratio_status(fail_ratio), 'time': update_time, 'description': 'Fail ratio: is ' + str(fail_ratio)})

        self.update_status(new_statuses)

    # ==========================================================================
    #                 Web
    # ==========================================================================    

    def lastStatus(self, incoming_params):
        response = {}
        try:
            default_params = {'delta': 8}
            params = self._create_params(default_params, incoming_params)
            result = []
            max_time = self.db_handler.get_session().query(func.max(self.tables['main'].c.time).label("max_time")).one()
            if max_time[0]:
                max_time = max_time[0]
                # query = self.tables['main'].select(self.tables['main'].c.time > max_time - timedelta(hours=params['delta']))
                query = self.tables['main'].select(self.tables['main'].c.time > Utils.get_UTC_now() - timedelta(hours=params['delta']))
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
    X = CMSJobStatus()
    X.ExecuteCheck()
    X.lastStatus({})

if __name__=='__main__':
    main()