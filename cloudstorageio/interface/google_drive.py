import os
import shutil

import cloudstorageio
import logging

from typing import Optional, Union
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from googleapiclient.errors import HttpError

from cloudstorageio.enums.enums import PrefixEnums
from cloudstorageio.tools.ci_collections import add_slash
from cloudstorageio.tools.logger import logger
from cloudstorageio.tools.decorators import timer
from cloudstorageio.configs import resources, CloudInterfaceConfig

# avoiding dependencies' warning
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


class GoogleDriveInterface:
    PREFIX = PrefixEnums.GOOGLE_DRIVE.value

    mimetypes_changes = {
        'application/vnd.google-apps.document': 'text/plain',
        'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}

    settings_file = os.path.abspath(os.path.join(os.path.dirname(resources.__file__), 'settings.yaml'))

    def __init__(self, **kwargs):

        self.config_file_path = kwargs.pop('google_cloud_credentials_path', None)

        if not self.config_file_path:
            self.config_file_path = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
        if not self.config_file_path:
            raise ValueError("Please add GOOGLE_DRIVE_CREDENTIALS environment variable"
                             " or set google_drive_credentials_json_path")

        self.tmp_folder = os.path.join(os.path.dirname(os.path.dirname(cloudstorageio.__file__)), 'gdrive_tmp')
        if os.path.isdir(self.tmp_folder):
            shutil.rmtree(self.tmp_folder)
        os.mkdir(path=self.tmp_folder)

        self.credentials = self._setup()
        self.drive = GoogleDrive(self.credentials)
        self._encoding = 'utf8'
        self._mode = None
        self._current_path = None
        self._write_mode = None
        self.metadata = None
        self.path = None
        self.recursive = False
        self.include_folders = False

    def _setup(self):

        self._set_configs()
        gauth = GoogleAuth(settings_file=self.settings_file)
        gauth.LocalWebserverAuth()
        return gauth

    def _set_configs(self):
        """"""
        with open(self.settings_file, 'rb') as f:
            yaml_string = f.read().decode()

        client_config_file = yaml_string.split('client_config_file:', 1)[1].split()[0]

        save_credentials_file = yaml_string.split('save_credentials_file:', 1)[1].split()[0]
        save_credentials_file_new = os.path.abspath(os.path.join(os.path.dirname(resources.__file__), 'credentials.json'))
        yaml_string = yaml_string.replace(client_config_file, self.config_file_path)
        yaml_string = yaml_string.replace(save_credentials_file, save_credentials_file_new)

        if client_config_file != self.config_file_path and os.path.isfile(save_credentials_file_new):
            os.remove(save_credentials_file_new)

        with open(self.settings_file, 'wb') as f:
            f.write(yaml_string.encode())

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
            value = value.split(self.PREFIX)[-1]

            if value in ('.', ''):
                self._current_path = ''
            else:
                self._current_path = value[:-1] if value.endswith('/') else value

    def get_id_from_full_path(self, name: str):
        """Get di from given file/folder full path"""

        def _find_id(file_obj_list: list, n):
            for i in file_obj_list:
                if i['title'] == n:
                    return i['id']
            return None

        file_id = None
        split_name_list = (name.split(self.PREFIX)[-1]).split('/')
        file_list = self.drive.ListFile({'q': "trashed=false"}).GetList()

        for file_idx, name in enumerate(split_name_list):
            file_id = _find_id(file_list, name)
            if not file_id:
                return None
            file_list = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(file_id)}).GetList()

        return file_id

    def get_full_path_from_id(self, file_id: str):
        """Get full path of given file(folder) id"""

        file_name = ''
        while True:
            file = self.drive.CreateFile({'id': file_id})
            # if not file.uploaded:
            #     raise FileNotFoundError(f'No such file/folder with id: {file_id}')

            if file['title'] != 'My Drive':
                file_name = os.path.join(file['title'], file_name)

            if file['parents']:
                parent_id = file['parents'][0]['id']
            else:
                return file_name[:-1]

            file_id = parent_id

    def _detect_path_type(self):
        """Detects whether given path is file, folder or does not exists"""

        if self.path == '':
            self._isdir = True
        if self._object_exists:
            drive_file_obj = self.drive.CreateFile({'id': self.id})
            if drive_file_obj['mimeType'] == 'application/vnd.google-apps.folder':
                self._isdir = True
            else:
                self._isfile = True

    def _populate_listdir(self, folder_id: str, parent: Optional[str] = ''):
        """Appends each file.folder name to self._listdir"""
        try:
            file_list = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(folder_id)}).GetList()
            for f in file_list:
                if f['mimeType'] == 'application/vnd.google-apps.folder':
                    p = parent + add_slash(f['title'])
                    if self.include_folders:
                        self._listdir.append(p)
                    if self.recursive:
                        self._populate_listdir(f['id'], parent=p)
                else:
                    full_path = os.path.join(parent, f['title'])
                    self._listdir.append(full_path)

        except HttpError:
            pass

    def _analyse_path(self, path: str):
        """From given path lists and detects object type (file/folder)"""

        self._isfile = False
        self._isdir = False
        self._listdir = list()
        self._object_exists = False

        self.path = path

        self.id = self.get_id_from_full_path(self.path)
        self._object_exists = True if self.id else False

        self._detect_path_type()
        self._populate_listdir(folder_id=self.id)

    def isfile(self, path: str):
        """Checks file existence for given path"""
        self._analyse_path(path)
        return self._isfile

    def isdir(self, path: str):
        """Checks dictionary existence for given path"""
        self._analyse_path(path)
        return self._isdir

    def listdir(self, path: str, recursive: Optional[bool] = False, exclude_folders: Optional[bool] = False):
        """Lists content for given folder path"""
        self.recursive = recursive
        self.include_folders = not exclude_folders
        self._analyse_path(path)

        if not self._object_exists:
            raise FileNotFoundError(f'No such file or dictionary: {path}')
        elif not self._isdir:
            raise NotADirectoryError(f"Not a directory: {path}")

        return self._listdir

    def remove(self, path: str):
        self._analyse_path(path)
        # TODO

    def open(self, path: str, mode: Optional[str] = None):
        """Opens a file from Google Drive and returns the GoogleDriveInterface object"""
        self._mode = mode
        self._analyse_path(path)
        return self

    def _create_folders(self, path: str):
        pass

    def write(self, content: Union[str, bytes], metadata: Optional[dict] = None):
        """Writes content to file on google drive
        :param content: The content that should be written to a file
        :param metadata:
        :return: String content of the file specified in the file path argument
        """

        if isinstance(content, str):
            content = content.encode(self._encoding)

        if not metadata:
            metadata = {}
        if self._mode is not None and ('w' not in self._mode and
                                       'a' not in self._mode and
                                       'x' not in self._mode and
                                       '+' not in self._mode):
            raise ValueError(f"Mode '{self._mode}' does not allow writing the file")

        if self._isfile:
            logger.info('Overwriting {} file'.format(self.path))
            file = self.drive.CreateFile({'id': self.id})
        else:
            folder_id = self.get_id_from_full_path(self.path.rsplit('/', 1)[0])
            if not folder_id:
                self._create_folders(self.path)
            file = self.drive.CreateFile({'title': self.path.rsplit('/')[-1], 'parents': [{"id": folder_id}]})

        tmp_file_path = os.path.join(self.tmp_folder, self.path.rsplit('/')[-1])

        with open(tmp_file_path, 'wb') as f:
            f.write(content)

        file.SetContentFile(tmp_file_path)
        file.Upload()
        os.remove(tmp_file_path)

    def read(self) -> Union[str, bytes]:
        """Reads google drive file and returns the bytes
        :return: String content of the file
        """
        if not self._isfile:
            raise FileNotFoundError('No such file: {}'.format(self.path))

        file = self.drive.CreateFile({'id': self.id})
        tmp_file_path = os.path.join(self.tmp_folder, self.path.rsplit('/')[-1])

        if file['mimeType'] in self.mimetypes_changes:
            download_mimetype = self.mimetypes_changes[file['mimeType']]
            file.GetContentFile(tmp_file_path, mimetype=download_mimetype)
        else:
            file.GetContentFile(tmp_file_path)
        with open(tmp_file_path, 'rb') as f:
            res = f.read()
        os.remove(tmp_file_path)

        if self._mode is not None and 'b' not in self._mode:
            try:
                res = res.decode(self._encoding)
            except UnicodeDecodeError:
                raise ValueError(f"The content cannot be decoded into a string"
                                 f" with encoding {self._encoding}."
                                 f" Include 'b' on read mode to return the original bytes")
        return res

    def __enter__(self):
        self._is_open = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._is_open = False
        self.path = None
