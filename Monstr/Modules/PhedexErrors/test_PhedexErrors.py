from Monstr.Core import Runner

def test_PhedexErrors_initial():
    modules = Runner.get_modules()
    assert 'PhedexErrors' in modules
    if 'PhedexErrors' in modules:
         modules['PhedexErrors'].main()

def test_RESTs():
    from Monstr.Modules.PhedexErrors.PhedexErrors import PhedexErrors
    obj = PhedexErrors()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name](obj, {})
