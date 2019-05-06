def test_status_timeline():
    from Monstr.Modules.StatusTimeline.StatusTimeline import StatusTimeline
    test_obj = StatusTimeline()
    test_obj.Initialize()
    for rest_name in test_obj.rest_links:
        test_obj.rest_links[rest_name]({})
