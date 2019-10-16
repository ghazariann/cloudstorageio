import os
import unittest

from cloudstorageio.tests import resources
from cloudstorageio.configs.cloud_interface_config import CloudInterfaceConfig
from cloudstorageio.enums.prefix_enum import PrefixEnums
from cloudstorageio.interface.cloud_interface import CloudInterface


class TestCloudInterface(unittest.TestCase):
    """Tests all CloudInterface methods with all available services"""

    S3_BUCKET_NAME = 'test-cloudstorageio'
    GS_BUCKET_NAME = 'test-cloudstorageio'
    # Give credentials manually to CloudInterface or set environment variables with CloudInterfaceConfig
    config_path = '/home/vahagn/Dropbox/cognaize/cloudstorageio_creds.json'
    CloudInterfaceConfig.set_configs(config_json_path=config_path)

    ci = CloudInterface()

    TEST_FOLDER = 'TEST_CI'
    BINARY_MOON = 'moon.jpg'
    SAMPLE_FOLDER = 'sample_files'

    def setUp(self):
        """
      sample-files/    v/  sample.jpg  v
         sample.txt
        """
        self.local_test_folder = os.path.abspath(os.path.join(os.path.dirname(resources.__file__)))

        self.copy_file_gs = PrefixEnums.GOOGLE_CLOUD.value + os.path.join(self.GS_BUCKET_NAME,
                                                                          self.TEST_FOLDER, self.BINARY_MOON)

        self.copy_file_s3 = PrefixEnums.S3.value + os.path.join(self.S3_BUCKET_NAME,
                                                                self.TEST_FOLDER, self.BINARY_MOON)

        self.copy_file_dbx = PrefixEnums.DROPBOX.value + os.path.join(self.TEST_FOLDER,
                                                                      self.SAMPLE_FOLDER, self.BINARY_MOON)

    def test_identify_path_type(self):
        """Tries invalid path """
        invalid_prefix = 'dx:/'
        invalid_path = invalid_prefix + os.path.join(self.TEST_FOLDER,
                                                     self.SAMPLE_FOLDER, self.BINARY_MOON)

        self.assertRaises(ValueError, self.ci.identify_path_type, invalid_path)

    def test_fetch(self):
        """ """
        pass

    def test_save(self):
        """ """
        pass

    def test_isfile(self):
        """ """
        pass

    def test_isdir(self):
        """"""
        pass

    def test_remove(self):
        """ """
        pass

    def test_listdir(self):
        pass
        """ """

    def test_copy(self):
        pass
        """ """

    def test_move(self):
        pass
        """ """

    def test_copy_batch(self):
        pass
        """ """

    # def test_copy(self):
    #     # copy local file to gs
    #     self.ci.copy(from_path=self.local_pic, to_path=self.copy_file_gs)
    #     res1 = self.ci.isfile(self.copy_file_gs)
    #     self.assertEqual(res1, True)
    #
    #     # check file existence in s3
    #     res2 = self.ci.isfile(self.copy_file_s3)
    #     self.assertEqual(res2, False)
    #
    #     # copy gs file to s3
    #     self.ci.copy(from_path=self.copy_file_gs, to_path=self.copy_file_s3)
    #     res3 = self.ci.isfile(self.copy_file_s3)
    #     self.assertEqual(res3, True)
    #
    #     # copy local storage file to dropbox
    #     self.ci.copy(from_path=self.local_pic, to_path=self.copy_file_dbx)
    #     res4 = self.ci.isfile(self.copy_file_dbx)
    #     self.assertEqual(res4, True)
    #
    # def tearDown(self) -> None:
    #     # delete all created files
    #     self.ci.remove(self.copy_file_gs)
    #     self.ci.remove(self.copy_file_s3)
    #     self.ci.remove(self.copy_file_dbx)