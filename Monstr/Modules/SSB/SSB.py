#!/bin/python

import json

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

    status_list = [{'name': 'good_links', 'status': 'UNKNOWN', 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'site_readiness', 'status': 'UNKNOWN', 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'sam3_ce', 'status': 'UNKNOWN', 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'sam3_srm', 'status': 'UNKNOWN', 'time': Utils.get_UTC_now(), 'description': ''},
                   {'name': 'visible', 'status': 'UNKNOWN', 'time': Utils.get_UTC_now(), 'description': ''}]

    DATA_HOSTNAME = "http://dashb-ssb.cern.ch/dashboard/request.py/siteviewjson?view=default"
    COLUMN_NAMES_HOSTNAME = "http://dashb-ssb.cern.ch/dashboard/request.py/getheaders?view=default"

    def __init__(self):
        super(SSB, self).__init__()
        self.db_handler = DB.DBHandler()

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
        update_time = Utils.get_UTC_now()
        last_status = [x._asdict() for x in self.db_handler.get_session().query(self.status_table['status']).all()]
        last_status = dict([(str(x['name']), str(x['status'])) for x in last_status])
        current_status = []
        for site in data['main']:
            if 'Disk' not in site['site_name'] and 'Buffer' not in site['site_name']:
                links_status = 'GOOD' if site['good_links'] == 'OK' else 'WARNING'
                current_status.append({'name': 'good_links', 'status': links_status, 'description': 'Value is ' + site['good_links']})

                readiness_status = 'GOOD' if site['site_readiness'] == 'Ok' else 'WARNING'
                current_status.append({'name': 'site_readiness', 'status': readiness_status, 'description': 'Value is ' + site['site_readiness']})

                sam3ce_status = 'GOOD' if site['sam3_ce'] == 'OK' else 'WARNING'
                current_status.append({'name': 'sam3_ce', 'status': sam3ce_status, 'description': 'Value is ' + site['sam3_ce']})

                sam3_srm = 'GOOD' if site['sam3_srm'] == 'OK' else 'WARNING'
                current_status.append({'name': 'sam3_srm', 'status': sam3_srm, 'description': 'Value is ' + site['sam3_srm']})

                visible_status = 'GOOD' if site['visible'] == 'OK' else 'WARNING'
                current_status.append({'name': 'visible', 'status': visible_status, 'description': 'Value is ' + site['visible']})

                conn = self.db_handler.get_engine().connect()
                for status in current_status:
                    if last_status[status['name']] != status['status']:
                        update = self.status_table['status'].update().values(status=status['status'], time=update_time, description=status['description']).where(self.status_table['status'].c.name == status['name'])
                        conn.execute(update)

    # ==========================================================================
    #                 Web
    # ==========================================================================

    def lastStatus(self, incoming_params):
        response = {}
        try:
            default_params = {'site_name': ''}
            params = self._create_params(default_params, incoming_params)
            result = []

            if params['site_name'] == '':
                query = self.tables['main'].select()
            # site_name=T1_RU_JINR|T1_RU_JINR_Disk
            elif len(params['site_name'].split('|')) == 2:
                site_names = params['site_name'].split('|')
                query = self.tables['main'].select((self.tables['main'].c.site_name == site_names[0]) |
                                                   (self.tables['main'].c.site_name == site_names[1]))
            else:
                query = self.tables['main'].select(self.tables['main'].c.site_name == params['site_name'])

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
    X = SSB()
    X.ExecuteCheck()

if __name__ == '__main__':
    main()
