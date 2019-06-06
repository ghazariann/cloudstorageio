""" Class S3Interface handles with S3 Storage files
    S3Interface contains
                        'read' and 'write' methods each of them can be accessed by 'open' method
                        isfile and isdir methods for checking object status (file, folder)
                        listdir method for listing folder's content
                        remove method for removing file/folder

    Besides boto3 API itself doesn't have any concept of a "folder"
        in S3Interface you can differentiate file/folder like in local process


"""

import io
from typing import Tuple, Optional, Union

import boto3

from cloudstorageio.utils.logger import logger


class S3Interface:
    PREFIX = "s3://"

    def __init__(self):
        self._encoding = 'utf8'
        self._session = boto3.session.Session()
        self._s3 = self._session.resource('s3')
        self._mode = None
        self._current_bucket = None
        self._current_path = None
        self._bucket = None
        self._object = None
        self.path = None
        self._is_open = False

    @property
    def path(self):
        if self._current_bucket is None:
            raise ValueError("Bucket name is not set")
        if self._current_path is None:
            raise ValueError("Path name is not set")
        return f"{S3Interface.PREFIX}{self._current_bucket}/{self._current_path}"

    @path.setter
    def path(self, value):
        if value is None:
            self._current_path = None
            self._current_bucket = None
        else:
            self._current_bucket, self._current_path = self._parse_bucket(value)
            self._current_path_with_backslash = self._current_path + '/'

    def _detect_type_of_object_summary(self, object_summary_key: str):
        """Inner method for detecting given s3.ObjectSummary's type (file or folder)
        :param object_summary_key: name of object_summary
        :return:
        """

        if object_summary_key == self._current_path:
            self._isfile = True
        if self._current_path_with_backslash in object_summary_key:
            self._isdir = True

    def _populate_listdir(self, object_summary_key: str):
        """Inner method for populating self._listdir
        :param object_summary_key: name of object summary
        :return:
        """
        split_list = object_summary_key.split(self._current_path_with_backslash, 1)
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
        """From given path create bucket, object, object_summaries, differentiate, list and detect status for object
        :param path: full path of file/folder
        :return:
        """

        self._isfile = False
        self._isdir = False
        self._listdir = list()
        self._object_exists = False

        self.path = path

        self._bucket = self._s3.Bucket(self._current_bucket)
        self._object = self._bucket.Object(self._current_path)
        self._object_summary_list = list(self._bucket.objects.filter(Prefix=self._current_path))

        self._object_key_list = [obj.key for obj in self._object_summary_list]

        for key_name in self._object_key_list:
            self._populate_listdir(key_name)
            self._detect_type_of_object_summary(key_name)

        if self._isdir or self._isfile:
            self._object_exists = True

    def isfile(self, path: str):
        self._process(path)
        return self._isfile

    def isdir(self, path: str):
        self._process(path)
        return self._isdir

    def listdir(self, path: str):
        """Check given dictionary type and list all object of it
        :param path: full path of s3 object (file/folder)
        :return:
        """
        self._process(path)
        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')
        elif not self._isdir:
            raise NotADirectoryError(f"Not a directory: {path}")

        return self._listdir

    def remove(self, path: str):
        """Check given path type and remove object(s) if found any
        :param path: full path of s3 object (file/folder)
        :return:
        """
        self._process(path)
        if not self._object_exists:
            raise FileNotFoundError(f"Object with path {path} does not exists")

        for obj in self._object_summary_list:
            obj.delete()

    def open(self, path: str, mode: Optional[str] = None, *args, **kwargs):
        """Open a file from s3 and return the S3Interface object"""
        self._mode = mode
        self._process(path)
        return self

    def read(self) -> Union[str, bytes]:
        """ Read S3 file and return the bytes
        :return: String content of the file
        """
        if not self._isfile:
            raise FileNotFoundError(' No such file: {}'.format(self.path))

        res = self._object.get()['Body'].read()
        if self._mode is not None and 'b' not in self._mode:
            try:
                res = res.decode(self._encoding)
            except UnicodeDecodeError:
                raise ValueError(f"The content cannot be decoded into a string"
                                 f" with encoding {self._encoding}."
                                 f" Include 'b' on read mode to return the original bytes")
        return res

    def write(self, content: Union[str, bytes, io.IOBase], metadata: Optional[dict] = None):
        """ Write text to a file on s3
        :param content: The content that should be written to a file
        :param metadata:
        :return: String content of the file specified in the file path argument
        """
        if self._isfile:
            logger.info('Overwriting {} file'.format(self.path))
        if isinstance(content, str):
            content = content.encode('utf8')
        if not metadata:
            metadata = {}
        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")

        self._object.put(Body=content, Metadata=metadata)

    @staticmethod
    def _parse_bucket(path: str) -> Tuple[str, str]:
        """Given a path, return the bucket name and the file path as a tuple"""
        path = path.split(S3Interface.PREFIX, 1)[-1]
        bucket_name, path = path.split('/', 1)
        return bucket_name, path

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None

