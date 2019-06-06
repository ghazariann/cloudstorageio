""" Class GoogleStorageInterface handles with Google Cloud Storage files

    Class GoogleStorageInterface has 'read' and 'write' methods each of them can be accessed by 'open' method
"""

import io
from typing import Tuple, Union, Optional

from google.cloud import storage

from cloudstorageio.utils.logger import logger


class GoogleStorageInterface:
    PREFIX = "gs://"

    def __init__(self):
        self._encoding = 'utf8'
        self._storage_client = storage.Client()
        self._mode = None
        self._current_bucket = None
        self._current_path = None
        self._bucket = None
        self._blob = None
        self.path = None
        self._is_open = False

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
            self._current_bucket, self._current_path = self._parse_bucket(value)
            self._current_path_with_backslash = self._current_path + '/'

    def _detect_blob_object_type(self, blob_name: str):
        """Inner method for detecting given blob object type (file or folder)
        :param blob_name: name of blob object
        :return:
        """
        if blob_name == self._current_path:
            self._isfile = True
        if self._current_path_with_backslash in blob_name:
            self._isdir = True

    def _populate_listdir(self, blob_name):
        """Append each blob inner name to self._listdir
        :param blob_name: storage.blob.Blob object
        :return:
        """

        split_list = blob_name.split(self._current_path_with_backslash, 1)
        if len(split_list) == 2:
            inner_object_name_list = split_list[-1].split('/', 1)

            if len(inner_object_name_list) == 2:
                # is dictionary
                inner_object_name = inner_object_name_list[0] + '/'
            else:
                # is file
                inner_object_name = inner_object_name_list[0]

            if inner_object_name != '' and inner_object_name not in self._listdir:
                self._listdir.append(inner_object_name)

    def _process(self, path: str):
        """From given path create bucket, blob objects, differentiate, list and detect type
        :param path: full path of file/folder
        :return:
        """

        self._isfile = False
        self._isdir = False
        self._listdir = list()
        self._object_exists = False

        self.path = path
        self._bucket = self._storage_client.get_bucket(self._current_bucket)
        self._blob_objects = list(self._bucket.list_blobs(prefix=self._current_path))

        self._blob_key_names_list = [obj.name for obj in self._blob_objects]

        for blob_name in self._blob_key_names_list:
            self._detect_blob_object_type(blob_name)
            self._populate_listdir(blob_name)

        if self._isdir or self._isfile:
            self._object_exists = True
        self._blob = self._bucket.get_blob(self._current_path)

    def isfile(self, path: str):
        self._process(path)
        return self._isfile

    def isdir(self, path: str):
        self._process(path)
        return self._isdir

    def listdir(self, path: str):
        """Check given dictionary's existence and list all object of it
        :param path: full path of gs object (file/folder)
        :return:
        :param path:
        :return:
        """
        self._process(path)
        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')
        elif not self._isdir:
            raise NotADirectoryError(f'Not a directory: {path}')

        return self._listdir

    def remove(self, path: str):
        """Check given path type and remove object(s) if found any
        :param path: full path of gs object (file/folder)
        :return:
        """
        self._process(path)
        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')

        for obj in self._blob_objects:
            obj.delete()

    def open(self, path: str, mode: Optional[str] = None, *args, **kwargs):
        """Open a file from gs and return the GoogleStorageInterface object"""
        self._mode = mode
        self._process(path)
        return self

    def read(self) -> Union[str, bytes]:
        """ Read gs file and return the bytes
        :return: String content of the file
        """
        if not self._isfile:
            raise FileNotFoundError('No such file: {}'.format(self.path))

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
        """ Write text to a file on google storage
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

    @staticmethod
    def _parse_bucket(path: str) -> Tuple[str, str]:
        """Given a path, return the bucket name and the file path as a tuple"""
        path = path.split(GoogleStorageInterface.PREFIX, 1)[-1]
        bucket_name, path = path.split('/', 1)
        return bucket_name, path

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None
