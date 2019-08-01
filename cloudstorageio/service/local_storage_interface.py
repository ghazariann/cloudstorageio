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

    def __init__(self, **kwargs):
        self._mode = None
        self.path = None
        self.recursive = False
        self.include_folders = False
        self._current_path = None
        self._current_path_with_backslash = None

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
            self._current_path = value[:-1] if (value.endswith('/') and value != '/') else value
            self._current_path_with_backslash = add_slash(self._current_path)

    def _populate_listdir(self):
        """Appends each file.folder name to self._listdir"""
        if self.recursive:
            for root, dirs, files in os.walk(self.path):
                for name in files:
                    self._listdir.append(os.path.join(root, name).split(self._current_path_with_backslash, 1)[1])
                if self.include_folders:
                    for name in dirs:
                        self._listdir.append(str(os.path.join(root, name).split(self._current_path_with_backslash, 1)[1])
                                             + '/')
        else:
            for i in os.listdir(self.path):
                if os.path.isdir(os.path.join(self.path, i)):
                    self._listdir.append(add_slash(i))
                else:
                    self._listdir.append(i)

    def _analyse_path(self, path: str):
        """From given path lists and detects object type (file/folder)"""
        self._isfile = False
        self._isdir = False
        self.recursive = False
        self.include_folders = False
        self._listdir = list()

        self.path = path
        self._isdir = os.path.isdir(self.path)
        self._isfile = os.path.isfile(self.path)

    def open(self, path: str, mode: Optional[str] = None, *args, **kwargs):
        """Opens a file from gs and return the GoogleStorageInterface object"""
        self._mode = mode
        self._analyse_path(path)
        return self

    def read(self) -> Union[str, bytes]:
        """ Reads gs file and return the bytes
        :return: String content of the file
        """
        if not self._isfile:
            raise FileNotFoundError('No such file: {}'.format(self.path))

        with open(self.path, self._mode) as f:
            res = f.read()
        return res

    def write(self, content: Union[str, bytes]):
        """ Writes text to a file on google storage
        :param content: The content that should be written to a file
        :return: String content of the file specified in the file path argument
        """
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
        except FileExistsError:
            logger.info(f'File/folder conflict for {os.path.dirname(self.path)} path')
            return None

        if self.isfile(self.path):
            logger.info('Overwriting {} file'.format(self.path))

        if isinstance(content, str):
            content = content.encode('utf8')
        try:
            with open(self.path, self._mode) as f:
                f.write(content)
        except IsADirectoryError:
            logger.info(f'File/folder conflict for {os.path.dirname(self.path)} path')

    def isfile(self, path: str):
        """Checks file existence for given path"""
        self._analyse_path(path)
        return self._isfile

    def isdir(self, path: str):
        """Checks dictionary existence for given path"""
        self._analyse_path(path)
        return self._isdir

    def remove(self, path: str):
        """Removes file/folder"""

        self._analyse_path(path)
        if self._isfile:
            os.remove(self.path)
        elif self._isdir:
            shutil.rmtree(self.path)
        else:
            raise FileNotFoundError(f'No such file or dictionary: {path}')

    def listdir(self, path: str, recursive: Optional[bool] = False, include_folders: Optional[bool] = False):
        """Lists all files/folders of dictionary"""

        self._analyse_path(path)
        self.recursive = recursive
        self.include_folders = include_folders

        if not self._isdir and not self._isfile:
            raise FileNotFoundError(f'No such file or dictionary: {self.path}')

        elif not self._isdir:
            raise NotADirectoryError(f"Not a directory: {self.path}")

        self._populate_listdir()
        return self._listdir

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None