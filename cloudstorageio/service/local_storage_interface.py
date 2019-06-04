""" Class LocalStorageInterface handles with local file/folder objects

    Class LocalStorageInterface contains
                                        open method, which is the same python built-in 'open' method
                                        isfile and isdir methods for checking object status
                                        listdir method for listing folder's content
                                        remove method for removing file or folder
"""
import os
import shutil


class LocalStorageInterface:

    @staticmethod
    def open(path, mode: str = 'rt', *args, **kwargs):
        return open(file=path, mode=mode, *args, **kwargs)

    @staticmethod
    def isfile(path: str):
        return os.path.isfile(path)

    @staticmethod
    def isdir(path: str):
        return os.path.isdir(path)

    def remove(self, path: str):
        """Check given path status and remove object(s) if found any
        :param path: full path of local file/folder
        :return:
        """
        if self.isfile(path):
            os.remove(path)
        elif self.isdir(path):
            shutil.rmtree(path)
        else:
            raise FileNotFoundError(f'No such file or dictionary: {path}')

    def listdir(self, path: str):
        """Check given dictionary status and list all object of its
        :param path: full path of local file/folder
        :return:
        """
        if not self.isdir(path) and not self.isfile(path):
            raise FileNotFoundError(f'No such file or dictionary: {path}')

        elif not self.isdir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        return os.listdir(path)


