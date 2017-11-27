from Monstr.Core import Runner


def test_PhedexErrors_initial():
    modules = Runner.get_modules()
    assert 'PhedexQuality' in modules
    if 'PhedexQuality' in modules:
        modules['PhedexQuality'].main()


def test_RESTs():
    from Monstr.Modules.PhedexQuality.PhedexQuality import PhedexQuality
    obj = PhedexQuality()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
