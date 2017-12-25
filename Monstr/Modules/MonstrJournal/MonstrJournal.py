#!/bin/python
from datetime import timedelta

from sqlalchemy import desc

import Monstr.Core.Utils as Utils
import Monstr.Core.DB as DB
import Monstr.Core.BaseModule as BaseModule


class MonstrJournal(BaseModule.BaseModule):
    name = 'MonstrJournal'

    config = {}

    def __init__(self):
        super(MonstrJournal, self).__init__()
        self.db_handler = DB.DBHandler()
        self.rest_links['getRows'] = self.getRows

    def Initialize(self):
        pass

    # ==========================================================================
    #                 Web
    # ==========================================================================

    def getRows(self, incoming_params):
        response = {}
        try:
            default_params = {'delta': 8}
            params = self._create_params(default_params, incoming_params)
            result = []
            time_now = Utils.get_UTC_now()
            query = self.journal.select((self.journal.c.time > time_now - timedelta(hours=params['delta']))).order_by(desc(self.journal.c.time))
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
    X = MonstrJournal()
    X.ExecuteCheck()

if __name__ == '__main__':
    main()
