from Monstr.Core import Runner


def test_SSB_initial():
    modules = Runner.get_modules()
    assert 'SSB' in modules
    if 'SSB' in modules:
        modules['SSB'].main()


def test_SSB_additional():
    modules = Runner.get_modules()
    assert 'SSB' in modules
    if 'SSB' in modules:
        modules['SSB'].main()


def test_RESTs():
    from Monstr.Modules.SSB.SSB import SSB
    obj = SSB()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
