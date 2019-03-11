""" Class LocalStorageInterface handles with local files

    Class LocalStorageInterface  has open method, which is similar to python biult-in 'open' method
"""


class LocalStorageInterface:

    @staticmethod
    def open(file, mode: str = 'rt', *args, **kwargs):
        return open(file=file, mode=mode, *args, **kwargs)
