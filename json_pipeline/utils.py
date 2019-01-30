from importlib import import_module
from copy import deepcopy


# Copied from scrapy.utils.misc.load_object
def load_object(path):
    """Load an object given its absolute object path, and return it.
    object can be a class, function, variable or an instance.
    """

    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError("Error loading object '%s': not a full path" % path)

    module, name = path[:dot], path[dot+1:]
    mod = import_module(module)

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError("Module '%s' doesn't define any object named '%s'" % (module, name))

    return obj


def dict_to_text(d, args):
    """Converts a dictionary to a string representation, replacing args.target with result.
    Use args.separator as separator between key/val pairs.
    """
    result = []
    d = deepcopy(d)
    if args.target in d:
        for key, val in d[args.target].items():
            result.append(f"{key}: {val}")
        d[args.target] = args.separator.join(result)
    return d
