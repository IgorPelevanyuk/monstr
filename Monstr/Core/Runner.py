import pkgutil
from importlib import import_module
import sys
import Monstr.Modules as Modules

def get_modules():
    modules = {}
    for importer, modname, ispkg in pkgutil.iter_modules(Modules.__path__):
        current = importer.find_module(modname).load_module(modname)
        for cur_importer, cur_modname, cur_ispkg in pkgutil.iter_modules(current.__path__):
            if 'test_' not in cur_modname:
                modules[cur_modname] = import_module('Monstr.Modules.' + modname + '.' + cur_modname)

    return modules

def main():
    args = sys.argv
    print 'Runner manually started'
    if (len(args) < 2):
        sys.exit()
    else:
        target = args[1]
        modules = get_modules()
        print modules
        if target in modules:
            print 'Test'
            print dir(modules[target])
            modules[target].InsertToDB()
        else:
            print "Module not found in %s" % modules.keys()

if __name__ == '__main__':
    main()