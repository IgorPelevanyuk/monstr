from abc import ABCMeta, abstractmethod
import Monstr.Core.DB as DB
import Monstr.Core.Utils as Utils
import Monstr.Core.Constants as Constants

# ,----------------------.
# |BaseNodule            |
# |----------------------|
# |+string name          |
# |+obj table_schemas    |
# |----------------------|
# |+void Initialize()    |
# |+obj PrepareRetrieve()|
# |+obj Retrieve()       |
# |+obj InsertToDB()     |
# |+obj Analyze()        |
# |+obj React()          |
# |+obj Run()            |
# `----------------------'


class BaseModule():
    __metaclass__ = ABCMeta
    
    name = None
    table_schemas = None
    tables = None
    status_table = None
    status_list = []
    events_table = None

    rest_links = {}

    db_handler = None

    status_schema = {'status': (DB.Column('id', DB.Integer, primary_key=True),
                                DB.Column('name', DB.String(64)),
                                DB.Column('status', DB.Integer),
                                DB.Column('time', DB.DateTime(True)),
                                DB.Column('description', DB.Text),)}

    journal_schema = (DB.Column('id', DB.Integer, primary_key=True),
                      DB.Column('module', DB.String(64)),
                      DB.Column('time', DB.DateTime(True)),
                      DB.Column('result', DB.String(32)),
                      DB.Column('step', DB.String(32)),
                      DB.Column('description', DB.Text),)

    events_schema = (DB.Column('id', DB.BigInteger, primary_key=True),
                     DB.Column('module', DB.String(64)),
                     DB.Column('name', DB.String(64)),
                     DB.Column('type', DB.String(32)),
                     DB.Column('time', DB.DateTime(True)),
                     DB.Column('severity', DB.Integer),
                     DB.Column('description', DB.Text),)

    # ==========================================================================
    # Database functions
    # ==========================================================================

    def _db_incert_journal_row(self, row):
        self.db_handler.insert(self.journal, row)

    def _db_incert_event_row(self, row):
        self.db_handler.insert(self.events_table, row)

    def _db_get_status_table_repr(self):
        return [x._asdict() for x in self.db_handler.get_session().query(self.status_table['status']).all()]

    def _db_update_status(self, statuses):
        conn = self.db_handler.get_engine().connect()
        for status in statuses:
            update = self.status_table['status'].update().values(status=status['status'], time=status['time'], description=status['description']).where(self.status_table['status'].c.name == status['name'])
            conn.execute(update)

    # ==========================================================================
    # Common functions
    # ==========================================================================

    def _create_journal_row(self, result, step=None, description=None):
        row = {'module': self.name,
               'time': Utils.get_UTC_now(),
               'result': result,
               'step': step,
               'description': description}
        return row

    def write_to_journal(self, result, step=None, description=None):
        row = self._create_journal_row(result, step, description)
        self._db_incert_journal_row(row)

    def write_error_to_journal(self, result, step=None, error=None):
        description = (type(error).__name__ + ': ' + error.message) if error is not None else None
        self.write_to_journal(result, step, description)

    # --------------------------------------------------------------------------

    def create_event(self, name, event_type, time, severity, description):
        event = {'module': self.name,
                 'name': name,
                 'type': event_type,
                 'time': time,
                 'severity': severity,
                 'description': description}
        return event

    def write_event(self, event):
        self._db_incert_event_row(event)

    # --------------------------------------------------------------------------

    def _create_params(self, default_params, params):
        result = {}
        for key in default_params:
            if key not in params:
                result[key] = default_params[key]
            else:
                result[key] = type(default_params[key])(params[key])
        return result

    def get_last_status(self):
        return self._db_get_status_table_repr()

    def update_status(self, current_statuses):
        previous_status = self._db_get_status_table_repr()
        last_status = dict([(str(x['name']), int(x['status'])) for x in previous_status])
        update_list = []
        event_list = []
        for status in current_statuses:
            if last_status[status['name']] != status['status']:
                update_list.append(status)
                # Generate event and write to DB
                event_name = status['name'] + ':' + Constants.STATUS[last_status[status['name']]] + '->' + Constants.STATUS[status['status']]
                event = self.create_event(event_name, 'StatusChange', status['time'], status['status'], status['description'])
                event_list.append(event)
                self.write_event(event)
        self._db_update_status(update_list)
        return event_list

    # --------------------------------------------------------------------------

    def __init__(self):
        self.db_handler = DB.DBHandler()
        self.rest_links = {'getModuleStatus': self.GetModuleStatus}

    def Initialize(self):
        if self.name is None:
            raise "Module require name"
        if self.table_schemas is None:
            raise "Module require schemas list"
        self.tables = self.db_handler.initialize(self.table_schemas, self.name)
        self.status_table = self.db_handler.initialize(self.status_schema, self.name, self.status_list)

    def PrepareRetrieve(self):
        return {}

    def Retrieve(self, params):
        pass

    def Analyze(self, data):
        return []

    def React(self, events):
        pass

    def InsertToDB(self, data):
        for schema in data:
            table = self.tables[schema]
            self.db_handler.bulk_insert(table, data[schema])
        return

    def ExecuteCheck(self):
        self.journal = self.db_handler.getOrCreateTable('monstr_Journal', self.journal_schema)
        self.events_table = self.db_handler.getOrCreateTable('monstr_Events', self.events_schema)

        try:
            self.Initialize()
        except Exception as e:
            self.write_error_to_journal('Fail', 'Initialize', e)
            print e
            return

        try:
            params = self.PrepareRetrieve()
        except Exception as e:
            self.write_error_to_journal('Fail', 'PrepareRetrieve', e)
            print e
            return

        try:
            data = self.Retrieve(params)
        except Exception as e:
            self.write_error_to_journal('Fail', 'Retrieve', e)
            print e
            return

        try:
            self.InsertToDB(data)
        except Exception as e:
            self.write_error_to_journal('Fail', 'InsertToDB', e)
            print e
            return

        #try:
        events = self.Analyze(data)
        #except Exception as e:
        #    self.write_error_to_journal('Fail', 'Analyze', e)
        #    print 'Analyze error'
        #    print e
        #    return

        try:
            self.React(data)
        except Exception as e:
            self.write_error_to_journal('Fail', 'React', e)
            print e
            return

        self.write_to_journal('Success')

    # ==========================================================================
    #                 Web
    # ==========================================================================

    def GetModuleStatus(self, incoming_params):
        response = {}
        params = incoming_params
        try:
            result = self._db_get_status_table_repr()
            response = {'data': result,
                        'applied_params': params,
                        'success': True}
        except Exception as e:
            response = {'data': result,
                        'incoming_params': incoming_params,
                        'success': False,
                        'error': type(e).__name__ + ': ' + e.message,
                        'description': 'Error inside BaseModule.GetModuleStatus'}

        return response
