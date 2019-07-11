import os
import unittest

from cloudstorageio.service.cloud_interface import CloudInterface


class TestCloudInterface(unittest.TestCase):

    def setUp(self):
        """
        for the beginning make sure to have this structure in your google cloud and s3 storage
                                                                          sample-files/    v/  sample.jpg  v
                                                                             sample.txt
        """
        self.ci = CloudInterface()

        self.local_test_folder = os.path.join(os.path.dirname(os.getcwd()), 'resources')
        self.local_pic = os.path.join(self.local_test_folder, 'Moon.jpg')
        self.copy_file_gs = 'gs://test-cloudstorageio/copy-moon.jpg'
        self.copy_file_s3 = 's3://test-cloudstorageio/copy-moon.jpg'
        self.copy_file_dbx = 'dbx://sample_files/copy-moon.jpg'

    def test_copy(self):
        # copy local file to gs
        self.ci.copy(from_path=self.local_pic, to_path=self.copy_file_gs)
        res1 = self.ci.isfile(self.copy_file_gs)
        self.assertEqual(res1, True)

        # check file existence in s3
        res2 = self.ci.isfile(self.copy_file_s3)
        self.assertEqual(res2, False)

        # copy gs file to s3
        self.ci.copy(from_path=self.copy_file_gs, to_path=self.copy_file_s3)
        res3 = self.ci.isfile(self.copy_file_s3)
        self.assertEqual(res3, True)

        # copy local storage file to dropbox
        self.ci.copy(from_path=self.local_pic, to_path=self.copy_file_dbx)
        res4 = self.ci.isfile(self.copy_file_dbx)
        self.assertEqual(res4, True)

    def tearDown(self) -> None:
        # delete all created files
        self.ci.remove(self.copy_file_gs)
        self.ci.remove(self.copy_file_s3)
        self.ci.remove(self.copy_file_dbx)