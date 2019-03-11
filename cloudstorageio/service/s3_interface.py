""" Class S3Interface handles with S3 Storage files
    S3Interface has 'read' and 'write' methods each of them can be accessed by 'open' method
"""

import io
from contextlib import contextmanager
from typing import Tuple, Optional, Union

import boto3


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

    @contextmanager
    def open(self, file: str, mode: Optional[str] = None, *args, **kwargs):
        """Open a file from s3 and return the S3Interface object"""
        self._mode = mode
        self.path = file
        try:
            yield self
        finally:
            self.path = None

    def read(self) -> Union[str, bytes]:
        """ Read S3 file and return the bytes
        :return: String content of the file
        """
        self._bucket = self._s3.Bucket(self._current_bucket)
        self._object = self._bucket.Object(self._current_path)
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
        :return: String content of the file specified in the filepath argument
        """
        if isinstance(content, str):
            content = content.encode('utf8')
        if not metadata:
            metadata = {}
        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")
        self._bucket = self._s3.Bucket(self._current_bucket)
        self._object = self._bucket.Object(self._current_path)
        self._object.put(Body=content, Metadata=metadata)

    @staticmethod
    def _parse_bucket(path: str) -> Tuple[str, str]:
        """Given a path, return the bucket name and the filepath as a tuple"""
        path = path.split(S3Interface.PREFIX, 1)[-1]
        bucket_name, path = path.split('/', 1)
        return bucket_name, path

