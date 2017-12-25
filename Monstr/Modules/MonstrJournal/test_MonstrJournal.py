def test_RESTs():
    from Monstr.Modules.MonstrJournal.MonstrJournal import MonstrJournal
    obj = MonstrJournal()
    for rest_name in obj.rest_links:
        obj.rest_links[rest_name]({})
