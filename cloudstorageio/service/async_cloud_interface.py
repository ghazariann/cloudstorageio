import os
import queue

from typing import Optional
from threading import Thread
from cloudstorageio import CloudInterface
from cloudstorageio.utils.timer import timer


class AsyncCloudInterface:

    def __init__(self, aws_region_name: Optional[str] = None, aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None, dropbox_token: Optional[str] = None,
                 dropbox_root: Optional[bool] = True, google_cloud_credentials_path: Optional[str] = None,
                 google_drive_credentials_path: Optional[str] = None, **kwargs):

        """Initializes AsyncCloudInterface instance
        :param aws_region_name: region name for S3 storage
        :param aws_access_key_id: access key id for S3 storage
        :param aws_secret_access_key: secret access key for S3 storage
        :param dropbox_token: generated token for dropbox app access
        :param google_cloud_credentials_path: local path of google cloud credentials file (json)
        :param google_drive_credentials_path: local path of google drive secret credentials file (json)
        :param kwargs:
        """

        self._kwargs = kwargs
        self.aws_region_name = aws_region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.dropbox_token = dropbox_token
        self.dropbox_root = dropbox_root
        self.google_cloud_credentials_path = google_cloud_credentials_path
        self.google_drive_credentials_path = google_drive_credentials_path

    def read_files(self, file_list: list, q: queue.Queue, from_folder_path: str):
        """
        :param file_list:
        :param q:
        :param from_folder_path:
        :return:
        """
        ci = CloudInterface(aws_region_name=self.aws_region_name, aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key, dropbox_token=self.dropbox_token,
                            dropbox_root=self.dropbox_root,
                            google_cloud_credentials_path=self.google_cloud_credentials_path,
                            google_drive_credentials_path=self.google_drive_credentials_path)

        for file_path in file_list:
            q.put((file_path, ci.fetch(os.path.join(from_folder_path, file_path))))
            print("Read file {} \n".format(file_path))

        q.put(('', ''))

    def write_files(self, q: queue.Queue, to_folder_path: str):
        """
        :param q:
        :param to_folder_path:
        :return:
        """
        while True:
            ci = CloudInterface(aws_region_name=self.aws_region_name, aws_access_key_id=self.aws_access_key_id,
                                aws_secret_access_key=self.aws_secret_access_key, dropbox_token=self.dropbox_token,
                                dropbox_root=self.dropbox_root,
                                google_cloud_credentials_path=self.google_cloud_credentials_path,
                                google_drive_credentials_path=self.google_drive_credentials_path)

            file_path, content = q.get(block=True)
            if not file_path:
                break

            ci.save(os.path.join(to_folder_path, file_path), content)
            print("Wrote file {} \n".format(file_path))
            q.task_done()

    @staticmethod
    def chunk_iterable(seq, num):
        avg = len(seq) / float(num)
        out = []
        last = 0.0

        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg

        return out

    @timer
    def copy_batch(self, from_folder_path: str, to_folder_path: str, process_amount: int = 10):

        ci = CloudInterface(aws_region_name=self.aws_region_name, aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key, dropbox_token=self.dropbox_token,
                            dropbox_root=self.dropbox_root,
                            google_cloud_credentials_path=self.google_cloud_credentials_path,
                            google_drive_credentials_path=self.google_drive_credentials_path)

        files_list = ci.listdir(from_folder_path, recursive=True)

        # print('listed')
        # if not process_amount:
        #     process_amount = len(files_list) / 2
        if process_amount > len(files_list):
            process_amount = len(files_list)

        chunks = self.chunk_iterable(files_list, process_amount)

        read_threads = []
        write_threads = []

        for chunk in chunks:
            # if chunk:

            q = queue.Queue()

            read_thread = Thread(target=self.read_files,
                                 args=(chunk, q, from_folder_path))

            read_threads.append(read_thread)

            write_thread = Thread(target=self.write_files, args=(q, to_folder_path))
            write_threads.append(write_thread)

            read_thread.start()
            write_thread.start()

        # q.join()

        for r, w in zip(read_threads, write_threads):
            r.join()
            w.join()
