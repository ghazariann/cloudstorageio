""" Class GoogleStorageInterface handles with Google Cloud Storage files/folders

    Class GoogleStorageInterface has
                                    read and write methods (can be accessed by open method)
                                    isfile and isdir methods for checking object status (file, folder)
                                    listdir method for listing folder's content
                                    remove method for removing file/folder
    Google Storage API itself doesn't have any concept of a "folder".
        In GoogleStorageInterface you can differentiate file/folder like in local environment
"""

import io
import os
from typing import Tuple, Union, Optional
from google.cloud import storage

from cloudstorageio.utils.interface import add_slash
from cloudstorageio.utils.logger import logger


class GoogleStorageInterface:
    PREFIX = "gs://"

    def __init__(self, **kwargs):
        """Initializes GoogleStorageInterface instance, creates storage client
        :param kwargs:
        """

        self.creds = kwargs.pop('google_credentials_json_path', None)
        if self.creds:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.creds
        if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ.keys():
            raise ConnectionRefusedError("Please add GOOGLE_APPLICATION_CREDENTIALS environment variable"
                                         " or set google_credentials_json_path")

        self._encoding = 'utf8'
        self._mode = None
        self._current_bucket = None
        self._current_path = None
        self._bucket = None
        self._blob = None
        self.path = None
        self._is_open = False
        self.only_bucket = False
        self._storage_client = storage.Client()

    @property
    def path(self):
        if self._current_bucket is None:
            raise ValueError("Bucket name is not set")
        if self._current_path is None:
            raise ValueError("Path name is not set")
        return f"{GoogleStorageInterface.PREFIX}{self._current_bucket}/{self._current_path}"

    @path.setter
    def path(self, value):
        if value is None:
            self._current_path = None
            self._current_bucket = None
        else:
            value = value[:-1] if value.endswith('/') else value
            self._current_bucket, self._current_path = self._parse_bucket(value)
            self._current_path_with_backslash = add_slash(self._current_path)

    def _detect_blob_object_type(self):
        """Hidden method for detecting given blob object type (file or folder)
        :return:
        """
        if self._current_path in self._blob_key_names_list:
            self._isfile = True
            self._blob_key_names_list.remove(self._current_path)

        if self._blob_key_names_list:
            self._isdir = True

    def _populate_listdir(self, blob_name):
        """Appends each blob inner name to self._listdir for bucket case
        :param blob_name: storage.blob.Blob object
        :return:
        """
        split_list = blob_name.split(self._current_path_with_backslash, 1)
        if len(split_list) == 2:
            inner_object_name = add_slash(split_list[0])
        else:
            inner_object_name = split_list[0]

        if inner_object_name not in self._listdir:
            self._listdir.append(inner_object_name)

    def _init_path(self, path: str):
        """Initializes path specific fields"""
        self._isfile = False
        self._isdir = False
        self._listdir = list()
        self._object_exists = False
        self.only_bucket = False

        self.path = path
        self._bucket = self._storage_client.get_bucket(self._current_bucket)

        self._blob_objects = self._bucket.list_blobs(prefix=self._current_path)

        self._blob_key_names_list = [obj.name for obj in self._blob_objects]

    def _analyse_path(self, path: str):
        """From given path creates bucket, blob objects, lists and detects object type (file/folder)
        :param path: full path of file/folder
        :return:
        """

        self._init_path(path)

        if self.only_bucket:
            self._isdir = True
        else:
            self._detect_blob_object_type()
            self._blob_key_names_list = [f.split(self._current_path_with_backslash, 1)[1] for f in self._blob_key_names_list
                                         if len(f.split(self._current_path_with_backslash, 1)) == 2]

        while '' in self._blob_key_names_list:
            self._blob_key_names_list.remove('')

        for blob in self._blob_key_names_list:
            self._populate_listdir(blob)

        if self._isdir or self._isfile:
            self._object_exists = True

    def isfile(self, path: str):
        """Checks file existence for given path"""
        self._analyse_path(path)
        return self._isfile

    def isdir(self, path: str):
        """Checks dictionary existence for given path"""
        self._analyse_path(path)
        return self._isdir

    def listdir(self, path: str, recursive: Optional[bool] = False, include_folders: Optional[bool] = False) -> list:
        """Checks given dictionary's existence and lists content
        :param path: full path of gs object (file/folder)
        :param recursive: list content fully
        :param include_folders:
        :return:
        """
        self._analyse_path(path)

        if recursive:
            if include_folders:
                folders = [f for f in self._listdir if f.endswith('/')]
                result = self._blob_key_names_list + folders
            else:
                result = [f for f in self._blob_key_names_list if not f.endswith('/')]
        else:
            result = self._listdir

        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')
        elif not self._isdir:
            raise NotADirectoryError(f'Not a directory: {path}')

        return result

    def remove(self, path: str):
        """Removes file/folder"""
        self._analyse_path(path)
        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')

        blob_objects = self._bucket.list_blobs(prefix=self._current_path)

        for obj in blob_objects:
            obj.delete()

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

        self._blob = self._bucket.get_blob(self._current_path)

        res = self._blob.download_as_string()

        if self._mode is not None and 'b' not in self._mode:
            try:
                res = res.decode(self._encoding)
            except UnicodeDecodeError:
                raise ValueError(f"The content cannot be decoded into a string"
                                 f" with encoding {self._encoding}."
                                 f" Include 'b' on read mode to return the original bytes")
        return res

    def write(self, content: Union[str, bytes, io.IOBase]):
        """ Writes text to a file on google storage
        :param content: The content that should be written to a file
        :return: String content of the file specified in the file path argument
        """

        if self._isfile:
            logger.info('Overwriting {} file'.format(self.path))
        if isinstance(content, str):
            content = content.encode('utf8')

        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")
        blob = self._bucket.blob(self._current_path)
        blob.upload_from_string(content)

    def _parse_bucket(self, path: str) -> Tuple[str, str]:
        """Given a path, return the bucket name and the file path as a tuple"""
        path = path.split(GoogleStorageInterface.PREFIX, 1)[-1]
        try:
            bucket_name, path = path.split('/', 1)
        except ValueError:
            bucket_name, path = path.split('/', 1)[0], ''
            self.only_bucket = True

        return bucket_name, path

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None
