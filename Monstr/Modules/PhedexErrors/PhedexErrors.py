#!/bin/python

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule

from datetime import timedelta
import json

from Monstr.Core.DB import Column, Integer, String, DateTime, Text, UniqueConstraint
from sqlalchemy.sql import func


class PhedexErrors(BaseModule.BaseModule):
    name = 'PhedexErrors'
    table_schemas = {'main': (Column('id', Integer, primary_key=True),
                              Column('instance', String(10)),
                              Column('from_site', String(60)),
                              Column('to_site', String(60)),
                              Column('transfer_log', Text),
                              Column('detail_log', Text),
                              Column('validate_log', Text),
                              Column('from_pfn', Text),
                              Column('to_pfn', Text),
                              Column('time_done', DateTime(True)),
                              UniqueConstraint("instance", "from_site", "to_site", "time_done"),)
                    }

    HOSTNAME = "http://cmsweb.cern.ch"
    REQUESTS = {"prod":{"from":"/phedex/datasvc/json/prod/ErrorLog?from=T1_RU_JINR*",
                      "to":"/phedex/datasvc/json/prod/ErrorLog?to=T1_RU_JINR*"},
                "debug":{"from":"/phedex/datasvc/json/debug/ErrorLog?from=T1_RU_JINR*",
                       "to":"/phedex/datasvc/json/debug/ErrorLog?to=T1_RU_JINR*"}
               }

    def __init__(self):
        super(PhedexErrors, self).__init__()
        self.db_handler = DB.DBHandler()
        self.rest_links['lastStatus'] = self.lastStatus

    def PrepareRetrieve(self):
        last_row = self.db_handler.get_session().query(func.max(self.tables['main'].c.time_done).label("max_time_done")).one()

        if last_row[0]:
            horizon = last_row[0] - timedelta(hours=3)
            #errors = Retrieve(horizon)
            avaliable_errors = self.db_handler.get_session().query(self.tables['main']).filter(self.tables['main'].c.time_done > (horizon).date()).all()
        else:
            horizon = Utils.epoch_to_datetime(0)
            #errors = Retrieve()
            avaliable_errors = []
        return {'horizon': horizon,
                'avaliable_errors': avaliable_errors}

    def Retrieve(self, params):

        retrieved_errors = []
        values = []
        for instance in self.REQUESTS:
            for direction in self.REQUESTS[instance]:
                json_raw = Utils.get_page(self.HOSTNAME+ self.REQUESTS[instance][direction])
                json_obj = json.loads(json_raw)['phedex']['link']
                for links in json_obj:
                    for blocks in links['block']:
                        for files in blocks['file']:
                            for errors in files['transfer_error']:
                                error = {'instance': str(instance),
                                         'from_site': str(links['from']),
                                         'to_site': str(links['to']),
                                         'from_pfn': str(errors['from_pfn']),
                                         'time_done': Utils.epoch_to_datetime(errors['time_done']),
                                         'detail_log': str(errors['detail_log']['$t']),                                         
                                         'transfer_log': str(errors['transfer_log']['$t']),
                                         'validate_log': str(errors['validate_log']['$t']),
                                         'to_pfn': str(errors['to_pfn']),
                                         #'transfer_code': int(errors['transfer_code']),
                                         #'time_xfer': Utils.epoch_to_datetime(errors['time_xfer']),
                                         #'time_inxfer': Utils.epoch_to_datetime(errors['time_inxfer']),
                                         #'time_assign': Utils.epoch_to_datetime(errors['time_assign']),
                                         #'report_code': int(errors['report_code']),
                                         #'time_export': Utils.epoch_to_datetime(errors['time_export']),
                                        }
                                if (tuple(error.values()) not in values) and (error['time_done'] > params['horizon']):
                                    values.append(tuple(error.values()))
                                    retrieved_errors.append(error)
        insert_list = []
        total = len(retrieved_errors)
        print total
        count = 0
        for error in retrieved_errors:
            if (not any((error['instance']==elem.instance 
                    and error['from_site']==elem.from_site 
                    and error['to_site']==elem.to_site 
                    and error['time_done']==elem.time_done) for elem in params['avaliable_errors'])):
                insert_list.append(error)
                count += 1
                if count % 1000 == 0:
                    print count, ' of ', total, ' done'
        return {'main': insert_list}


    #==========================================================================
    #                 Web
    #==========================================================================    

    def lastStatus(self, incoming_params):
        response = {}
        try:
            default_params = {'delta': 8}
            params = self._create_params(default_params, incoming_params)
            result = []
            max_time = self.db_handler.get_session().query(func.max(self.tables['main'].c.time_done).label("max_time")).one()
            if max_time[0]:
                max_time = max_time[0]
                query = self.tables['main'].select(self.tables['main'].c.time_done > max_time - timedelta(hours=params['delta']))
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
                        'default_params': [[key, default_params[key], type(default_params[key]) ] for key in default_params],
                        'success': False,
                        'error': type(e).__name__ + ': ' + e.message}

        return response


def main():
    X = PhedexErrors()
    X.ExecuteCheck()
    
if __name__=='__main__':
    main()