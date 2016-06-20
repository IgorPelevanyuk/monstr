from abc import ABCMeta, abstractmethod
import Monstr.Core.DB as DB



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
    db_handler = None

    def __init__(self):
        self.db_handler = DB.DBHandler()

    def Initialize(self):
        if self.name is None:
            raise "Module require name"
        if self.table_schemas is None:
            raise "Module require schemas list"
        self.tables = self.db_handler.initialize(self.table_schemas, self.name)


    def PrepareRetrieve(self):
        return {}

    @abstractmethod
    def Retrieve(self, params):
        pass

    def InsertToDB(self, data):
        for schema in data:
            table = self.tables[schema]
            self.db_handler.bulk_insert(table, data[schema])        

    def ExecuteCheck(self):
        print 'ExecuteCheck'
        self.Initialize()
        params = self.PrepareRetrieve()
        data = self.Retrieve(params)
        self.InsertToDB(data)
