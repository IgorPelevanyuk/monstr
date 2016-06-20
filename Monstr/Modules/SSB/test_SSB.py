from Monstr.Core import Runner

modules = Runner.get_modules()
assert 'SSB' in modules
if 'SSB' in modules:
     modules['SSB'].InsertToDB()