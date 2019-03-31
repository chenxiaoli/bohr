from __future__ import absolute_import

import importlib
import os
import sys

from utils.dotdict import dotdict

from bitmex_trade import _settings_base as baseSettings


def import_path(fullpath):
    """
    Import a file with full path specification. Allows one to
    import from anywhere, something __import__ does not do.
    """
    path, filename = os.path.split(fullpath)
    filename, ext = os.path.splitext(filename)
    sys.path.insert(0, path)
    module = importlib.import_module(filename, path)
    importlib.reload(module)  # Might be out of date
    del sys.path[0]
    return module


userSettings = import_path(os.path.join('.', 'settings'))


# Assemble settings.
settings = {}
settings.update(vars(baseSettings))
settings.update(vars(userSettings))


# Main export
settings = dotdict(settings)