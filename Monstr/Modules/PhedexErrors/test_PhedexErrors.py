def PhedexErrors_Manual():
    from Monstr.Modules.PhedexErrors.PhedexErrors import PhedexErrors
    test_obj = PhedexErrors()
    test_obj.Initialize()
    params = test_obj.PrepareRetrieve()
    data = test_obj.Retrieve(params)
    test_obj.InsertToDB(data)
    events = test_obj.Analyze(data)
    test_obj.React(data)


def test_PhedexErrors_initial():
    import Monstr.Modules.PhedexErrors.PhedexErrors as PhedexErrors
    PhedexErrors.main()


def test_RESTs():
    from Monstr.Modules.PhedexErrors.PhedexErrors import PhedexErrors
    obj = PhedexErrors()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
