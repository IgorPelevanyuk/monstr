import mock

def test_Runner():
    import sys
    import Monstr.Core.Runner as Runner

    with mock.patch.object(sys, 'argv', ['/path_do_not_needed_for_test', 'CMSJobStatus']):
        Runner.main()