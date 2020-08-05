""" Class S3Interface handles with S3 Storage files/folders
    S3Interface has
                        read and write methods (can be accessed by open method)
                        isfile and isdir methods for checking object status (file, folder)
                        listdir method for listing folder's content
                        remove method for removing file/folder

    Boto3 API itself doesn't have any concept of a "folder".
        In S3Interface you can differentiate file/folder like in local environment

"""

import io
import os
from typing import Tuple, Optional, Union

import boto3

from cloudstorageio.enums.enums import PrefixEnums
from cloudstorageio.tools.ci_collections import add_slash
from cloudstorageio.tools.logger import logger


class S3Interface:
    PREFIX = PrefixEnums.S3.value

    def __init__(self, **kwargs):
        """Initializes S3Interface instance, creates session and resources for given credentials
        :param kwargs:
        """

        # try to find necessary parameters from given kwargs, if not, assign none
        self._region = kwargs.pop('aws_region_name', None)
        self._acc_key = kwargs.pop('aws_access_key_id', None)
        self._acc_secret_key = kwargs.pop('aws_secret_access_key', None)
        if not self._region:
            self._region = os.environ.get('AWS_REGION_NAME')
        if not self._acc_key:
            self._acc_key = os.environ.get('AWS_ACCESS_KEY')
        if not self._acc_secret_key:
            self._acc_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

        # check given two access keys mutually existence
        if (self._acc_secret_key and not self._acc_key) or (self._acc_key and not self._acc_secret_key):
            raise ConnectionRefusedError('Please provide both aws_access_key_id and aws_secret_access_key')

        self._session = boto3.session.Session(aws_access_key_id=self._acc_key,
                                              aws_secret_access_key=self._acc_secret_key,
                                              region_name=self._region)

        self._encoding = 'utf8'
        self._s3 = self._session.resource('s3')
        self._mode = None
        self._current_bucket = None
        self._current_path = None
        self._bucket = None
        self._object = None
        self.path = None
        self._is_open = False
        self.only_bucket = False

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
            value = value[:-1] if value.endswith('/') else value
            self._current_bucket, self._current_path = self._parse_bucket(value)
            self._current_path_with_backslash = add_slash(self._current_path)

    @staticmethod
    def get_bucket_region(bucket_name):
        """Get region name of specific bucket
        :param bucket_name: name of s3 bucket object
        :return:
        """
        return boto3.client('s3').get_bucket_location(Bucket=bucket_name)['LocationConstraint']

    def _detect_blob_object_type(self):
        """Hidden method for detecting given blob object type (file or folder)
        :return:
        """
        if self._current_path in self._object_key_list:
            self._isfile = True
            self._object_key_list.remove(self._current_path)

        if self._object_key_list:
            self._isdir = True

    def _populate_listdir(self, blob_name):
        """Appends each blob inner name to self._listdir for bucket case
        :param blob_name: storage.blob.Blob object
        :return:
        """
        split_list = blob_name.split('/', 1)
        if len(split_list) == 2:
            inner_object_name = add_slash(split_list[0])
        else:
            inner_object_name = split_list[0]

        if inner_object_name not in self._listdir:
            self._listdir.append(inner_object_name)

    def _init_path(self, path):
        """Initializes path specific fields"""
        self._isfile = False
        self._isdir = False
        self._listdir = list()
        self._object_exists = False

        self.path = path

        self._bucket = self._s3.Bucket(self._current_bucket)
        self._object = self._bucket.Object(self._current_path)
        self._object_summary_list = self._bucket.objects.filter(Prefix=self._current_path)

        self._object_key_list = [obj.key for obj in self._object_summary_list]

    def _analyse_path(self, path: str):
        """From given path create bucket, object, object_summaries, list and identify object type (file/folder)
        :param path: full path of file/folder
        :return:
        """
        self._init_path(path)

        if self.only_bucket:
            self._isdir = True
        else:
            self._object_key_list = [f.split(self._current_path_with_backslash, 1)[-1] for f in
                                     self._object_key_list]
            self._detect_blob_object_type()

        while '' in self._object_key_list:
            self._object_key_list.remove('')

        for key_name in self._object_key_list:
            self._populate_listdir(key_name)

        if self._isdir or self._isfile:
            self._object_exists = True

    def isfile(self, path: str) -> bool:
        """Checks file existence for given path"""
        self._analyse_path(path)
        return self._isfile

    def isdir(self, path: str) -> bool:
        """Checks dictionary existence for given path"""
        self._analyse_path(path)
        return self._isdir

    def listdir(self, path: str, recursive: Optional[bool] = False, exclude_folders: Optional[bool] = False) -> list:
        """Lists content for given folder path"""
        self._analyse_path(path)
        include_folders = not exclude_folders
        if recursive:
            if include_folders:
                folders_list = [f for f in self._listdir if f.endswith('/')]
                result = self._object_key_list + folders_list
            else:
                result = [f for f in self._object_key_list if not f.endswith('/')]

        else:
            if include_folders:
                result = self._listdir
            else:
                result = [f for f in self._listdir if not f.endswith('/')]

        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')
        elif not self._isdir:
            raise NotADirectoryError(f"Not a directory: {path}")

        return result

    def remove(self, path: str) -> None:
        """Deletes file/folder"""
        self._analyse_path(path)
        if not self._object_exists:
            raise FileNotFoundError(f"Object with path {path} does not exists")

        for obj in self._object_summary_list:
            obj.delete()

    def open(self, path: str, mode: Optional[str] = None):
        """Opens a file from s3 and return the S3Interface object"""
        self._mode = mode
        self._analyse_path(path)
        return self

    def read(self) -> Union[str, bytes]:
        """Reads S3 file and return the bytes
        :return: String content of the file
        """
        if not self._isfile:
            raise FileNotFoundError('No such file: {}'.format(self.path))

        res = self._object.get()['Body'].read()
        if self._mode is not None and 'b' not in self._mode:
            try:
                res = res.decode(self._encoding)
            except UnicodeDecodeError:
                raise ValueError(f"The content cannot be decoded into a string"
                                 f" with encoding {self._encoding}."
                                 f" Include 'b' on read mode to return the original bytes")
        return res

    def write(self, content: Union[str, bytes, io.IOBase], metadata: Optional[dict] = None,
              acl: Optional[str] = 'private'):

        """Writes text to a file on s3
        :param content: The content that should be written to a file
        :param metadata: Metadata for file
        :param acl: access control permission for written file ('private' by default)
        :return: String content of the file specified in the file path argument
        """
        if self._isfile:
            ...
            # logger.info('Overwriting {} file'.format(self.path))
        if isinstance(content, str):
            content = content.encode('utf8')
        if not metadata:
            metadata = {}
        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")

        self._object.put(ACL=acl, Body=content, Metadata=metadata)

    def _parse_bucket(self, path: str) -> Tuple[str, str]:
        """Given a path, return the bucket name and the file path as a tuple"""
        path = path.split(S3Interface.PREFIX, 1)[-1]
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
