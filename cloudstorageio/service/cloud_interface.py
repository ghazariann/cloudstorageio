""" Class CloudInterface handles with Google Cloud Storage and S3 storage objects(files/folder)
                                (combination of GoogleStorageInterface and S3Interface classes)
    All methods are for all 3 environments (s3, google cloud, local)

    Class CloudInterface contains
                                open method, for opening/creating given file object
                                isfile and isdir methods for checking object status (file, folder)
                                listdir method for listing folder's content
                                remove method for removing file/folder
                                copy method for copying file from one storage to another

    See the followings for more information about features used in code:
        https://www.youtube.com/watch?v=-aKFBoZpiqA&t=596sP
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html
        https://googleapis.github.io/google-cloud-python/latest/storage/
"""

import os
from cloudstorageio.service.google_storage_interface import GoogleStorageInterface
from cloudstorageio.service.local_storage_interface import LocalStorageInterface
from cloudstorageio.service.s3_interface import S3Interface

from typing import Optional


class CloudInterface:

    def __init__(self, region_name: Optional[str] = None, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None, **kwargs):

        self._kwargs = kwargs
        self._kwargs['region_name'] = region_name
        self._kwargs['aws_access_key_id'] = aws_access_key_id
        self._kwargs['aws_secret_access_key'] = aws_secret_access_key

        self._filename = None
        self._mode = None
        self._s3 = None
        self._gs = None
        self._local = None
        self._current_storage = None
        self._path = None

    def identify_path_type(self, path: str):
        """Identify type of given path and create class object for that type
        :param path: full path of file
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
        else:
            raise ValueError(f"`{path}` is invalid")

    @staticmethod
    def is_local_path(path: str) -> bool:
        return os.path.exists(path) or os.path.isdir(os.path.dirname(path))

    @staticmethod
    def is_s3_path(path: str) -> bool:
        if path.startswith(S3Interface.PREFIX):
            return True
        return False

    @staticmethod
    def is_google_storage_path(path: str) -> bool:
        if path.startswith(GoogleStorageInterface.PREFIX):
            return True
        return False

    def open(self, file_path, mode='rt', *args, **kwargs):
        self.identify_path_type(file_path)
        return self._current_storage.open(path=file_path, mode=mode, *args, **kwargs)

    def isfile(self, path: str):
        self.identify_path_type(path)
        return self._current_storage.isfile(path)

    def isdir(self, path: str):
        self.identify_path_type(path)
        return self._current_storage.isdir(path)

    def remove(self, path: str):
        self.identify_path_type(path)
        return self._current_storage.remove(path)

    def listdir(self, path: str):
        self.identify_path_type(path)
        return self._current_storage.listdir(path)

    def copy(self, source: str, to: str):
        """ Copy given file to new destination
        :param source: local or remote storage path of existing file
        :param to: local or remote storage path of new file
        :return:
        """
        with self.open(source, 'rb') as f:
            content = f.read()
        with self.open(to, 'wb') as f:
            f.write(content)
