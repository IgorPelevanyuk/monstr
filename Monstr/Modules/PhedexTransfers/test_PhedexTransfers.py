from Monstr.Core import Runner


def test_PhedexErrors_initial():
    modules = Runner.get_modules()
    assert 'PhedexTransfers' in modules
    if 'PhedexTransfers' in modules:
        modules['PhedexTransfers'].main()


def test_RESTs():
    from Monstr.Modules.PhedexTransfers.PhedexTransfers import PhedexTransfers
    obj = PhedexTransfers()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
