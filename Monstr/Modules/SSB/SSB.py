#!/bin/python

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule

from datetime import datetime
import json

from Monstr.Core.DB import Column, Integer, String, Text

class SSB(BaseModule.BaseModule):
    name = 'SSB'    
    table_schemas = {'main': (Column('id', Integer, primary_key=True),
                              Column('time', Text),
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
                              Column('running', String(20)),
                              Column('pending', String(20)),
                              Column('in_rate_phedex', String(20)),
                              Column('out_rate_phedex', String(20)),
                              Column('topologymaintenances', Text),
                              Column('ggus', String(20)),)
                    }

    DATA_HOSTNAME = "http://dashb-ssb.cern.ch/dashboard/request.py/siteviewjson?view=default"
    COLUMN_NAMES_HOSTNAME = "http://dashb-ssb.cern.ch/dashboard/request.py/getheaders?view=default"
    
    def __init__(self):
        super(SSB, self).__init__()
        self.db_handler = DB.DBHandler()

    def Retrieve(self, params):
        retrieve_time = str(datetime.now())
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
                    result[site_name][column_names[index]] = value if value != '' else 'n/a'
                result[site_name]['time'] = retrieve_time
                result[site_name]['site_name'] = str(site_name)

        insert_list = []
        for data in result:
            insert_list.append(result[data])

        return {'main': insert_list}

def InsertToDB():
    X = SSB()
    X.ExecuteCheck()

if __name__=='__main__':
    InsertToDB()