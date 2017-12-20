from Monstr.Core import Runner


def test_SSB_initial():
    import Monstr.Modules.SSB.SSB as testedModule
    testedModule.main()


def test_SSB_additional():
    import Monstr.Modules.SSB.SSB as testedModule
    testedModule.main()


def test_SSB_manual():
    from Monstr.Modules.SSB.SSB import SSB
    test_obj = SSB()
    test_obj.Initialize()
    params = test_obj.PrepareRetrieve()
    data = test_obj.Retrieve(params)
    test_obj.InsertToDB(data)
    events = test_obj.Analyze(data)
    test_obj.React(data)


def test_RESTs():
    from Monstr.Modules.SSB.SSB import SSB
    obj = SSB()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
