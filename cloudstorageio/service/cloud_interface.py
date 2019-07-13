""" Class CloudInterface handles with Google Cloud Storage, S3 and Dropbox storage files/folder
    All methods are for all 4 environments (s3, google cloud, dropbox, local)

    Class CloudInterface contains
                                open method, for opening/creating given file object
                                isfile and isdir methods for checking object status (file, folder)
                                listdir method for listing folder's content
                                remove method for removing file/folder
                                copy method for copying file from one storage to another
"""

import os
from cloudstorageio.service.google_storage_interface import GoogleStorageInterface
from cloudstorageio.service.local_storage_interface import LocalStorageInterface
from cloudstorageio.service.s3_interface import S3Interface
from cloudstorageio.service.dropbox_interface import DropBoxInterface

from typing import Optional, Callable


class CloudInterface:

    def __init__(self, aws_region_name: Optional[str] = None, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None, dropbox_token: Optional[str] = None, **kwargs):
        """Initializes CloudInterface instance
        :param aws_region_name: region name for S3 storage
        :param aws_access_key_id: access key id for S3 storage
        :param aws_secret_access_key: secret access key for S3 storage
        :param dropbox_token: generated token for dropbox app access
        :param kwargs:
        """

        self._kwargs = kwargs
        self._kwargs['aws_region_name'] = aws_region_name
        self._kwargs['aws_access_key_id'] = aws_access_key_id
        self._kwargs['aws_secret_access_key'] = aws_secret_access_key
        self._kwargs['dropbox_token'] = dropbox_token

        self._filename = None
        self._mode = None
        self._s3 = None
        self._gs = None
        self._local = None
        self._dbx = None
        self._current_storage = None
        self._path = None

    def identify_path_type(self, path: str):
        """Identifies "type" of given path and create class instance
        :param path: full path of file/folder
        :return: None
        """
        self._path = path.strip()

        if self.is_local_path(self._path):
            if self._local is None:
                self._local = LocalStorageInterface()
            self._current_storage = self._local

        elif self.is_s3_path(self._path):
            if self._s3 is None:
                self._s3 = S3Interface(**self._kwargs)
            self._current_storage = self._s3

        elif self.is_google_storage_path(self._path):
            if self._gs is None:
                self._gs = GoogleStorageInterface(**self._kwargs)
            self._current_storage = self._gs

        elif self.is_dropbox_path(self._path):
            if self._dbx is None:
                self._dbx = DropBoxInterface(**self._kwargs)
            self._current_storage = self._dbx
        else:
            raise ValueError(f"`{path}` is invalid. Please use dbx:// prefix for dropbox, s3:// for S3 storage"
                             f" and gs:// for Google Cloud Storage ")

    @staticmethod
    def is_local_path(path: str) -> bool:
        """Checks if the given path is for local storage"""
        return os.path.exists(path) or os.path.isdir(os.path.dirname(path))

    @staticmethod
    def is_s3_path(path: str) -> bool:
        """Checks if the given path is for S3 storage"""
        return path.startswith(S3Interface.PREFIX)

    @staticmethod
    def is_google_storage_path(path: str) -> bool:
        """Checks if the given path is for google storage"""
        return path.startswith(GoogleStorageInterface.PREFIX)

    @staticmethod
    def is_dropbox_path(path: str) -> bool:
        """Checks if the given path is for dropBox"""
        return path.startswith(DropBoxInterface.PREFIX)

    def open(self, file_path: str, mode: Optional[str] = 'rt', *args, **kwargs) -> Callable:
        """Identifies given file path and return "open" method for detected current storage"""
        self.identify_path_type(file_path)
        return self._current_storage.open(path=file_path, mode=mode, *args, **kwargs)

    def isfile(self, path: str) -> Callable:
        """Checks file existence for given path"""
        self.identify_path_type(path)
        return self._current_storage.isfile(path)

    def isdir(self, path: str) -> Callable:
        """Checks dictionary existence for given path"""
        self.identify_path_type(path)
        return self._current_storage.isdir(path)

    def remove(self, path: str) -> Callable:
        """Deletes file/folder"""
        self.identify_path_type(path)
        return self._current_storage.remove(path)

    def listdir(self, path: str):
        """Lists all files/folders containing in given folder path"""
        self.identify_path_type(path)
        return self._current_storage.listdir(path)

    def copy(self, from_path: str, to_path: str):
        """Copies given file to new destination
        :param from_path: local or remote storage path of existing file
        :param to_path: local or remote storage path of new file
        :return:
        """
        with self.open(from_path, 'rb') as f:
            content = f.read()
        with self.open(to_path, 'wb') as f:
            f.write(content)


if __name__ == '__main__':
    ci = CloudInterface()
    ci.isfile('s3://test/fgss/')
    print('zfdg')
    print('zfdg')