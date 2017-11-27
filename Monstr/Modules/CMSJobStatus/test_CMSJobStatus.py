import mock


def test_CMSJobStatus_beforeLastHour():
    import datetime
    import pytz
    import Monstr.Modules.CMSJobStatus.CMSJobStatus as testedModule

    with mock.patch("Monstr.Modules.CMSJobStatus.CMSJobStatus.Utils.get_UTC_now") as MockClass:
        MockClass.return_value = datetime.datetime.utcnow().replace(tzinfo=pytz.utc) - datetime.timedelta(hours=1)
        testedModule.main()


def test_CMSJobStatus_addLastHour():
    import Monstr.Modules.CMSJobStatus.CMSJobStatus as testedModule
    testedModule.main()


def test_RESTs():
    from Monstr.Modules.CMSJobStatus.CMSJobStatus import CMSJobStatus
    obj = CMSJobStatus()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
