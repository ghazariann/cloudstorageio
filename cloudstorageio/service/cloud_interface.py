""" Class CloudInterface handles with Google Cloud Storage, S3 and Dropbox storage files/folder
    All methods are for all 4 environments (s3, google cloud, dropbox, local)

    Class CloudInterface contains
                                open method, for opening/creating given file object
                                isfile and isdir methods for checking object status (file, folder)
                                listdir method for listing folder's content
                                remove method for removing file/folder
                                copy method for copying file from one storage to another
"""
import enum
import functools
import multiprocessing
import os
from multiprocessing.pool import Pool
from typing import Optional, Callable

from cloudstorageio.service.google_storage_interface import GoogleStorageInterface
from cloudstorageio.service.local_storage_interface import LocalStorageInterface
from cloudstorageio.service.s3_interface import S3Interface
from cloudstorageio.service.dropbox_interface import DropBoxInterface
from cloudstorageio.service.google_drive_interface import GoogleDriveInterface

from cloudstorageio.utils.decorators import timer, storage_cache_factory
from cloudstorageio.utils.interface_functions import path_formatter
from cloudstorageio.utils.logger import logger


class CloudInterface:

    class PrefixEnums(enum.Enum):
        S3 = 's3://'
        GOOGLE_CLOUD = 'gs://'
        DROPBOX = 'dbx://'
        DRIVE = 'gdrive://'

    def __init__(self, aws_region_name: Optional[str] = None, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None, dropbox_token: Optional[str] = None,
                 dropbox_root: Optional[bool] = False, google_cloud_credentials_path: Optional[str] = None,
                 google_drive_credentials_path: Optional[str] = None, **kwargs):

        """Initializes CloudInterface instance
        :param aws_region_name: region name for S3 storage
        :param aws_access_key_id: access key id for S3 storage
        :param aws_secret_access_key: secret access key for S3 storage
        :param dropbox_token: generated token for dropbox app access
        :param dropbox_root: namespace id starts from root
        :param google_cloud_credentials_path: local path of google cloud credentials file (json)
        :param google_drive_credentials_path: local path of google drive secret credentials file (json)
        :param kwargs:
        """

        self._kwargs = kwargs
        self._kwargs['aws_region_name'] = aws_region_name
        self._kwargs['aws_access_key_id'] = aws_access_key_id
        self._kwargs['aws_secret_access_key'] = aws_secret_access_key
        self._kwargs['dropbox_token'] = dropbox_token
        self._kwargs['dropbox_root'] = dropbox_root
        self._kwargs['google_cloud_credentials_path'] = google_cloud_credentials_path
        self._kwargs['google_drive_credentials_path'] = google_drive_credentials_path

        self._filename = None
        self._mode = None
        self._s3 = None
        self._gs = None
        self._local = None
        self._dbx = None
        self._dr = None
        self._current_storage = None
        self._path = None

    def identify_path_type(self, path: str):
        """Identifies "type" of given path and create class instance
        :param path: full path of file/folder
        :return: None
        """

        self._path = path_formatter(path)

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

        elif self.is_drive_path(self._path):
            if self._dr is None:
                self._dr = GoogleDriveInterface(**self._kwargs)
            self._current_storage = self._dr
        else:
            raise ValueError(f"`{path}` is invalid. Please use {self.PrefixEnums.DROPBOX.value} prefix for dropBox,"
                             f" {self.PrefixEnums.S3.value} for S3 storage, "
                             f" {self.PrefixEnums.GOOGLE_CLOUD.value} for Google Cloud Storage or VALID local path")

    def _reset_fields(self):
        """Set all instance attributes to none"""
        self._filename = None
        self._mode = None
        self._s3 = None
        self._gs = None
        self._local = None
        self._dbx = None
        self._dr = None
        self._current_storage = None
        self._path = None

    @staticmethod
    def is_local_path(path: str) -> bool:
        """Checks if the given path is for local storage"""
        is_dir = False
        path = path.strip()
        while not is_dir and path != '':
            is_dir = os.path.isdir(path)
            path = os.path.dirname(path)
        return is_dir

    @classmethod
    def is_s3_path(cls, path: str) -> bool:
        """Checks if the given path is for S3 storage"""
        return path.strip().startswith(cls.PrefixEnums.S3.value)

    @classmethod
    def is_google_storage_path(cls, path: str) -> bool:
        """Checks if the given path is for google storage"""
        return path.strip().startswith(cls.PrefixEnums.GOOGLE_CLOUD.value)

    @classmethod
    def is_dropbox_path(cls, path: str) -> bool:
        """Checks if the given path is for dropBox"""
        return path.strip().startswith(cls.PrefixEnums.DROPBOX.value)

    @classmethod
    def is_drive_path(cls, path: str) -> bool:
        """Checks if the given path is for google drive"""
        return path.strip().startswith(cls.PrefixEnums.DRIVE.value)

    def open(self, file_path: str, mode: Optional[str] = 'rt', *args, **kwargs) -> Callable:
        """Identifies given file path and return "open" method for detected current storage"""
        self.identify_path_type(file_path)
        res = self._current_storage.open(path=file_path, mode=mode, *args, **kwargs)
        self._reset_fields()
        return res

    def save(self, path: str, content):
        """Save content to given file"""
        with self.open(path, 'wb') as f:
            f.write(content)

    def fetch(self, path: str) -> bytes:
        """Fetch data of given file"""
        with self.open(path, 'rb') as f:
            res = f.read()
        return res

    def isfile(self, path: str) -> Callable:
        """Checks file existence for given path"""
        self.identify_path_type(path)
        res = self._current_storage.isfile(path)
        self._reset_fields()
        return res

    def isdir(self, path: str) -> Callable:
        """Checks dictionary existence for given path"""
        self.identify_path_type(path)
        res = self._current_storage.isdir(path)
        self._reset_fields()
        return res

    def remove(self, path: str) -> Callable:
        """Deletes file/folder"""
        self.identify_path_type(path)
        res = self._current_storage.remove(path)
        self._reset_fields()
        return res

    def listdir(self, path: str, recursive: Optional[bool] = False, include_folders: Optional[bool] = False) -> list:
        """ Lists all files/folders containing in given folder path
        :param path: the full path of folder (with prefix)
        :param recursive: list folder recursively, (by default no)
        :param include_folders: include folders in list (by default no, lists only files)
        :return: list of folder's content (file/folder names)
        """
        self.identify_path_type(path)
        res = self._current_storage.listdir(path=path, recursive=recursive,
                                            include_folders=include_folders)
        self._reset_fields()
        return res

    @storage_cache_factory()
    def cache_listdir(self, path: str, recursive: Optional[bool] = False, include_folders: Optional[bool] = False):
        """Cache the output of first call, then use the cached output (when called again)"""
        return self.listdir(path=path, recursive=recursive, include_folders=include_folders)

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

    def move(self, from_path: str, to_path: str):
        """Moves given file to new destination"""
        self.copy(from_path=from_path, to_path=to_path)
        self.remove(path=from_path)
        logger.info(f'Moved {from_path} file to {to_path}')

    def _call_copy(self, p, from_path, to_path):
        """call copy with from/to full paths"""
        try:
            full_from_path = os.path.join(from_path, p)
            full_to_path = os.path.join(to_path, p)
            self.copy(from_path=full_from_path, to_path=full_to_path)
            logger.info(f'Copied {full_from_path} file to {full_to_path}')

        except Exception as e:
            logger.info(e, p)

    @timer
    def copy_batch(self, from_path: str, to_path: str, multiprocess: Optional[bool] = True,
                   continue_copy: Optional[bool] = False):
        """ Copy entire batch(folder) to new destination
        :param from_path: folder/bucket to copy from
        :param to_path: name of folder to copy files
        :param multiprocess: indicator of doing process with multiprocess(faster) or with simple for loop
        :param continue_copy:
        :return:
        """
        if continue_copy:
            from_path_list = self.listdir(from_path, recursive=True)

            to_path_list = self.listdir(to_path, recursive=True)
            full_path_list = list(set(from_path_list) - set(to_path_list))
        else:
            full_path_list = self.listdir(from_path, recursive=True)

        if multiprocess:
            p = Pool(multiprocessing.cpu_count())
            # for each call from_path and to_path are the same
            partial_func = functools.partial(self._call_copy, from_path=from_path, to_path=to_path)
            p.map(partial_func, full_path_list)
        else:
            for f in full_path_list:
                from_full = os.path.join(from_path, f)
                to_full = os.path.join(to_path, f)
                logger.info(f'Copied {from_full} file to {to_full}')
                self.copy(from_full, to_full)


if __name__ == '__main__':
    ci = CloudInterface()
    print(ci.listdir('dbx://', recursive=False, include_folders=False))
