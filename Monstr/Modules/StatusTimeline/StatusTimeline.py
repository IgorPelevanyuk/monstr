#!/bin/python
from datetime import timedelta

from sqlalchemy import asc

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule


class StatusTimeline(BaseModule.BaseModule):
    name = 'StatusTimeline'

    config = {}

    def __init__(self):
        super(StatusTimeline, self).__init__()
        self.db_handler = DB.DBHandler()
        self.rest_links['getEventsLog'] = self.getEventsLog

    def Initialize(self):
        pass

    # ==========================================================================
    #                 Web
    # ==========================================================================
    # select * FROM "monstr_Events" t1 JOIN (SELECT module, state, max(time) as maxtime from "monstr_Events" where time < current_date - interval '2' day group by module, state) t2 on t1.module=t2.module and t1.state=t2.state and t1.time=t2.maxtime;
    def getEventsLog(self, incoming_params):
        response = {}
        try:
            default_params = {'delta': 48}
            params = self._create_params(default_params, incoming_params)
            result = []
            time_now = Utils.get_UTC_now()
            query = self.events_table.select((self.events_table.c.time > time_now - timedelta(hours=params['delta']))).order_by(asc(self.events_table.c.time))
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
    X = StatusTimeline()
    X.ExecuteCheck()

if __name__ == '__main__':
    main()
