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

from cloudstorageio.utils.logger import logger


class LocalStorageInterface:

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
        else:
            self._current_path = value

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

        if self.isfile(self.path):
            logger.info('Overwriting {} file'.format(self.path))
        #
        # folder = self.path.rsplit('/', 1)[-1]
        # path = folder
        # while not self.isdir(path):
        #     path = path.rsplit('/', 1)[-1]
        #
        # while path != folder:
        #     os.mkdir(path)
        #     node = (folder.split(path, 1)[1]).split("/", 1)[0]
        #     folder = os.path.join(folder, node)
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

    def list_recursive(self, path):
        """List paths' of all inner file/folder objects"""
        if not self.isdir(path) and not self.isfile(path):
            raise FileNotFoundError(f'No such file or dictionary: {path}')

        elif not self.isdir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        res = list()
        for root, dirs, files in os.walk(path):
            for name in files:
                res.append(os.path.join(root, name).split(path + '/', 1)[-1])
            for name in dirs:
                res.append((os.path.join(root, name).split(path + '/', 1)[-1]) + '/')

        return res

    def listdir(self, path: str):
        """Lists all files/folders of dictionary"""

        if not self.isdir(path) and not self.isfile(path):
            raise FileNotFoundError(f'No such file or dictionary: {path}')

        elif not self.isdir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        return os.listdir(path)

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None
