""" Class CloudInterface handles with Google Cloud Storage, S3 and Dropbox storage files/folder
    All methods are for all 4 environments (s3, google cloud, dropbox, local)

    Class CloudInterface contains
                                open method, for opening/creating given file object
                                isfile and isdir methods for checking object status (file, folder)
                                listdir method for listing folder's content
                                remove method for removing file/folder
                                copy method for copying file from one storage to another
"""
import functools
import multiprocessing
import os
from multiprocessing.pool import Pool
from typing import Optional, Callable

from cloudstorageio.enums import PrefixEnums
from cloudstorageio.interface import GoogleStorageInterface
from cloudstorageio.interface import LocalStorageInterface
from cloudstorageio.interface import S3Interface
from cloudstorageio.interface import DropBoxInterface
from cloudstorageio.interface import GoogleDriveInterface

from cloudstorageio.tools.decorators import timer, storage_cache_factory
from cloudstorageio.tools.ci_collections import path_formatter
from cloudstorageio.tools.logger import logger


class CloudInterface:

    def __init__(self, aws_region_name: Optional[str] = None, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None, dropbox_token: Optional[str] = None,
                 dropbox_root: Optional[bool] = None, google_cloud_credentials_path: Optional[str] = None,
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
            raise ValueError(f"`{path}` is invalid. Please use {PrefixEnums.DROPBOX.value} prefix for dropBox,"
                             f" {PrefixEnums.S3.value} for S3 storage, "
                             f" {PrefixEnums.GOOGLE_CLOUD.value} for Google Cloud Storage,"
                             f"{PrefixEnums.GOOGLE_DRIVE.value} for Google Drive, or VALID local path")

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

    @staticmethod
    def is_s3_path(path: str) -> bool:
        """Checks if the given path is for S3 storage"""
        return path.strip().startswith(PrefixEnums.S3.value)

    @staticmethod
    def is_google_storage_path(path: str) -> bool:
        """Checks if the given path is for google storage"""
        return path.strip().startswith(PrefixEnums.GOOGLE_CLOUD.value)

    @staticmethod
    def is_dropbox_path(path: str) -> bool:
        """Checks if the given path is for dropBox"""
        return path.strip().startswith(PrefixEnums.DROPBOX.value)

    @staticmethod
    def is_drive_path(path: str) -> bool:
        """Checks if the given path is for google drive"""
        return path.strip().startswith(PrefixEnums.GOOGLE_DRIVE.value)

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

    def listdir(self, path: str, recursive: Optional[bool] = False, exclude_folders: Optional[bool] = False) -> list:
        """ Lists all files/folders containing in given folder path
        :param path: the full path of folder (with prefix)
        :param recursive: list folder recursively, (by default no)
        :param exclude_folders: exclude folders from list (by default no, lists folders too)
        :return: list of folder's content (file/folder names)
        """
        self.identify_path_type(path)
        res = self._current_storage.listdir(path=path, recursive=recursive, exclude_folders=exclude_folders)
        self._reset_fields()
        return res

    @storage_cache_factory()
    def cache_listdir(self, path: str, recursive: Optional[bool] = False, exclude_folders: Optional[bool] = False):
        """Cache the listed output of the first call, then use the already cached output (when called again)"""
        return self.listdir(path=path, recursive=recursive, exclude_folders=exclude_folders)

    def copy(self, from_path: str, to_path: str):
        """Copies given file to new destination"""
        if self.is_local_path(from_path) and self.is_s3_path(to_path):
            self.identify_path_type(to_path)  # setting up s3 storage
            with self.open(to_path, 'wb') as f:
                f.upload(from_path)
                return

        content = self.fetch(path=from_path)
        self.save(path=to_path, content=content)

    def move(self, from_path: str, to_path: str):
        """Moves given file to new destination"""

        self.copy(from_path=from_path, to_path=to_path)
        self.remove(path=from_path)

    def _call_copy(self, p, from_path, to_path):
        """Calls copy with full paths"""
        try:
            full_from_path = os.path.join(from_path, p)
            full_to_path = os.path.join(to_path, p)
            self.copy(from_path=full_from_path, to_path=full_to_path)
        except Exception as e:
            logger.error(f'Failed to copy {p} file : {e}')

    @timer
    def copy_dir(self, source_dir: str, dest_dir: str, multiprocess: Optional[bool] = True,
                 continue_copy: Optional[bool] = False):
        """ Recursively copy a directory
        :param source_dir: folder/bucket to copy from
        :param dest_dir: folder/bucket to copy to (dest_dir does not need to exist)
        :param multiprocess: indicator of multiprocessing
        :param continue_copy: if True, will ignore the same files existing in both dirs and copy only differing ones
        :return:
        """
        if continue_copy:
            from_path_list = self.listdir(source_dir, recursive=True, exclude_folders=True)

            try:
                to_path_list = self.listdir(dest_dir, recursive=True, exclude_folders=True)
            except FileNotFoundError:
                to_path_list = []

            full_path_list = list(set(from_path_list) - set(to_path_list))
        else:
            full_path_list = self.listdir(source_dir, recursive=True, exclude_folders=True)

        if multiprocess:
            p = Pool(multiprocessing.cpu_count())
            # for each call from_path & to_path are constants
            partial_func = functools.partial(self._call_copy, from_path=source_dir, to_path=dest_dir)
            p.map(partial_func, full_path_list)
        else:
            for f in full_path_list:
                self._call_copy(f, source_dir, dest_dir)

    def _call_copy_zip(self, from_to_zip: zip):
        try:
            self.copy(from_to_zip[0], from_to_zip[1])
        except Exception as e:
            logger.error(f'Failed to copy {from_to_zip[0]} file : {e}')

    def copy_batch(self, from_batch: list, to_batch: list, multiprocess: Optional[bool] = True):
        """ Copy entire batch (list)
        :param from_batch: folder/file path list to copy from
        :param to_batch: folder/file path list to copy to
        :param multiprocess: indicator of doing process with multiprocessing
        :return:
        """
        if multiprocess:
            p = Pool(multiprocessing.cpu_count())
            partial_func = functools.partial(self._call_copy_zip)
            p.map(partial_func, zip(from_batch, to_batch))
        else:
            for from_file, to_file in zip(from_batch, to_batch):
                self.copy(from_path=from_file, to_path=to_file)
