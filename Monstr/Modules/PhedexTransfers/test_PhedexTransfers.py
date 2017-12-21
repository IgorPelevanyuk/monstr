def PhedexTransfers_Manual():
    from Monstr.Modules.PhedexTransfers.PhedexTransfers import PhedexTransfers, DB
    test_obj = PhedexTransfers()
    # This workaround is required for TravisCI only.
    test_obj.status_schema = {'status': (DB.Column('id', DB.Integer, primary_key=True),
                                         DB.Column('name', DB.String(64)),
                                         DB.Column('status', DB.Integer),
                                         DB.Column('time', DB.DateTime(True)),
                                         DB.Column('description', DB.Text),)}

    test_obj.Initialize()
    params = test_obj.PrepareRetrieve()
    data = test_obj.Retrieve(params)
    test_obj.InsertToDB(data)
    events = test_obj.Analyze(data)
    test_obj.React(data)


def test_PhedexTransfers_initial():
    PhedexTransfers_Manual()


def test_PhedexTransfers_update():
    PhedexTransfers_Manual()


def test_RESTs():
    from Monstr.Modules.PhedexTransfers.PhedexTransfers import PhedexTransfers
    obj = PhedexTransfers()
    obj.Initialize()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
