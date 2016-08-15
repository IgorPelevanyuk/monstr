import pkgutil
import inspect
from importlib import import_module
import sys
import Monstr.Modules as Modules
from Monstr.Core.BaseModule import BaseModule as BaseModule

def get_modules():
    modules = {}
    for importer, modname, ispkg in pkgutil.iter_modules(Modules.__path__):
        current = importer.find_module(modname).load_module(modname)
        for cur_importer, cur_modname, cur_ispkg in pkgutil.iter_modules(current.__path__):
            if  modname == cur_modname:
                modules[cur_modname] = import_module('Monstr.Modules.' + modname + '.' + cur_modname)
    return modules

def main():
    args = sys.argv
    if (len(args) < 2):
        sys.exit()
    else:
        target = args[1]
        modules = get_modules()
        if target in modules:
            inner_classes = inspect.getmembers(modules[target], inspect.isclass)
            cls_found = False
            for cls in inner_classes:
                current_class = cls[1]
                is_cls_defined_in_target_module = current_class.__module__ == modules[target].__name__
                is_cls_is_BaseModule = BaseModule in inspect.getmro(current_class)
                if is_cls_defined_in_target_module and is_cls_is_BaseModule:
                    cls_found = True
                    obj = current_class()
                    obj.ExecuteCheck()
            if cls_found is False:
                print 'No BaseModule child class was found in target module %s' % modules[target].__name__ 
        else:
            print "Module not found in %s" % modules.keys()

if __name__ == '__main__':
    main()
