#!/bin/python

import json
from pprint import pprint as pp


import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule

from Monstr.Core.DB import Column, Integer, String, Text, DateTime


class SSB(BaseModule.BaseModule):
    name = 'SSB'
    table_schemas = {'main': (Column('id', Integer, primary_key=True),
                              Column('time', DateTime(True)),
                              Column('site_name', String(20)),
                              Column('visible', String(20)),
                              Column('active_t2s', String(20)),
                              Column('site_readiness', String(20)),
                              Column('hc_glidein', String(20)),
                              Column('sam3_ce', String(20)),
                              Column('sam3_srm', String(20)),
                              Column('good_links', String(20)),
                              Column('commissioned_links', String(20)),
                              Column('analysis', String(20)),
                              Column('running', Integer),
                              Column('pending', Integer),
                              Column('in_rate_phedex', Integer),
                              Column('out_rate_phedex', Integer),
                              Column('topologymaintenances', Text),
                              Column('ggus', Integer),)}

    status_list = [{'name': 'good_links', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'site_readiness', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'sam3_ce', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'sam3_srm', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'visible', 'status': 0, 'time': Utils.get_UTC_now(), 'description': ''}]

    DATA_HOSTNAME = "http://dashb-ssb.cern.ch/dashboard/request.py/siteviewjson?view=default"
    COLUMN_NAMES_HOSTNAME = "http://dashb-ssb.cern.ch/dashboard/request.py/getheaders?view=default"

    def __init__(self):
        super(SSB, self).__init__()
        self.db_handler = DB.DBHandler()
        self.rest_links['lastStatus'] = self.lastStatus

    def Initialize(self):
        if self.name is None:
            raise "Module require name"
        if self.table_schemas is None:
            raise "Module require schemas list"
        self.tables = self.db_handler.initialize(self.table_schemas, self.name)
        self.status_table = self.db_handler.initialize(self.status_schema, self.name, self.status_list)

    def Retrieve(self, params):
        retrieve_time = Utils.get_UTC_now().replace(second=0, microsecond=0)
        result = {'T1_RU_JINR': {}, 'T1_RU_JINR_Buffer': {}, 'T1_RU_JINR_Disk': {}}

        column_names = {}
        json_raw = Utils.get_page(self.COLUMN_NAMES_HOSTNAME)
        json_obj = json.loads(json_raw)['columns']

        for column in json_obj:
            temp_dict = {column['pos']: str(column['ColumnName']).lower().replace(' ', '_')}
            column_names.update(temp_dict)

        json_raw = Utils.get_page(self.DATA_HOSTNAME)
        json_obj = json.loads(json_raw)['aaData']

        for data in json_obj:
            site_name = data[0][2:]
            if site_name in result:
                for index in column_names:
                    value = str(data[index].split('|')[2])
                    result[site_name][column_names[index]] = value if value != '' else None
                result[site_name]['time'] = retrieve_time
                result[site_name]['site_name'] = str(site_name)

        insert_list = []
        for data in result:
            insert_list.append(result[data])

        return {'main': insert_list}

    def Analyze(self, data):
        new_statuses = []
        update_time = Utils.get_UTC_now()
        for site in data['main']:
            if 'Disk' not in site['site_name'] and 'Buffer' not in site['site_name']:
                links_status = 10 if site['good_links'] == 'OK' else 40
                new_statuses.append({'name': 'good_links', 'status': links_status, 'time': update_time, 'description': 'Good Links: is ' + str(site['good_links'])})

                readiness_status = 10 if site['site_readiness'] == 'Ok' else 40
                if site['site_readiness'] is None:
                    readiness_status = 0
                new_statuses.append({'name': 'site_readiness', 'status': readiness_status, 'time': update_time, 'description': 'Site Readiness: is ' + str(site['site_readiness'])})

                sam3ce_status = 10 if site['sam3_ce'] == 'OK' else 40
                new_statuses.append({'name': 'sam3_ce', 'status': sam3ce_status, 'time': update_time, 'description': 'SAM3 CE: is ' + str(site['sam3_ce'])})

                sam3_srm = 10 if site['sam3_srm'] == 'OK' else 40
                new_statuses.append({'name': 'sam3_srm', 'status': sam3_srm, 'time': update_time, 'description': 'SAM3 SRM: is ' + str(site['sam3_srm'])})

                visible_status = 10 if site['visible'] == 'OK' else 40
                new_statuses.append({'name': 'visible', 'status': visible_status, 'time': update_time, 'description': 'Visible: is ' + str(site['visible'])})

                self.update_status(new_statuses)

    # ==========================================================================
    #                 Web
    # ==========================================================================

    def lastStatus(self, incoming_params):
        response = {}
        try:
            default_params = {'site_name': 'T1_RU_JINR'}
            params = self._create_params(default_params, incoming_params)
            result = []

            last_row = self.db_handler.get_session().query(DB.func.max(self.tables['main'].c.time).label("max_time")).one()
            if len(last_row) > 0:
                query = self.tables['main'].select((self.tables['main'].c.site_name == params['site_name']) & (self.tables['main'].c.time == last_row[0]))

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
    X = SSB()
    X.ExecuteCheck()

if __name__ == '__main__':
    main()
