""" Class LocalStorageInterface handles with local file/folder objects

    Class LocalStorageInterface contains
                                        open method, which is the same python built-in 'open' method
                                        isfile and isdir methods for checking object status
                                        listdir method for listing folder's content
                                        remove method for removing file or folder
"""
import io
import os
import shutil
from typing import Optional, Union

from cloudstorageio.utils.interface import add_slash
from cloudstorageio.utils.logger import logger


class LocalStorageInterface:
    PREFIX = "lc:/"

    def __init__(self):
        self._mode = None
        self.path = None

    @property
    def path(self):
        if self._current_path is None:
            raise ValueError("Path name is not set")
        return self._current_path

    @path.setter
    def path(self, value):
        if value is None:
            self._current_path = None
            self._current_path_with_backslash = None
        else:
            value = value.split(self.PREFIX, 1)[-1]
            self._current_path = value[:-1] if value.endswith('/') else value
            self._current_path_with_backslash = add_slash(self._current_path)

    def open(self, path: str, mode: Optional[str] = None, *args, **kwargs):
        """Opens a file from gs and return the GoogleStorageInterface object"""
        self._mode = mode
        self.path = path
        return self

    def read(self) -> Union[str, bytes]:
        """ Reads gs file and return the bytes
        :return: String content of the file
        """
        with open(self.path, self._mode) as f:
            res = f.read()
        return res

    def write(self, content: Union[str, bytes, io.IOBase]):
        """ Writes text to a file on google storage
        :param content: The content that should be written to a file
        :return: String content of the file specified in the file path argument
        """
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
        except FileExistsError:
            os.remove(os.path.dirname(self.path))
            logger.info(f'File/folder conflict in {os.path.dirname(self.path)} path')
            os.makedirs(os.path.dirname(self.path), exist_ok=True)

        if self.isfile(self.path):
            logger.info('Overwriting {} file'.format(self.path))

        if isinstance(content, str):
            content = content.encode('utf8')

        with open(self.path, self._mode) as f:
            f.write(content)

    @staticmethod
    def isfile(path: str):
        """Checks file existence for given path"""
        return os.path.isfile(path)

    @staticmethod
    def isdir(path: str):
        """Checks dictionary existence for given path"""
        return os.path.isdir(path)

    def remove(self, path: str):
        """Removes file/folder"""

        if self.isfile(path):
            os.remove(path)
        elif self.isdir(path):
            shutil.rmtree(path)
        else:
            raise FileNotFoundError(f'No such file or dictionary: {path}')

    def listdir(self, path: str, recursive: Optional[bool] = False, include_recursive_folders: Optional[bool] = False):
        """Lists all files/folders of dictionary"""

        self.path = path

        if not self.isdir(path) and not self.isfile(path):
            raise FileNotFoundError(f'No such file or dictionary: {path}')

        elif not self.isdir(path):
            raise NotADirectoryError(f"Not a directory: {path}")
        res = list()

        if recursive:
            for root, dirs, files in os.walk(path):
                for name in files:
                    res.append(os.path.join(root, name).split(self._current_path_with_backslash, 1)[1])
                if include_recursive_folders:
                    for name in dirs:
                        res.append((os.path.join(root, name).split(self._current_path_with_backslash, 1)[1]) + '/')
            return res
        else:
            for i in os.listdir(path):
                if os.path.isdir(os.path.join(path, i)):
                    res.append(add_slash(i))
                else:
                    res.append(i)
            return res

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None
