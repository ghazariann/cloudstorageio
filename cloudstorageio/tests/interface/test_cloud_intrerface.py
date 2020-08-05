import os
import random
import unittest
import warnings

from cloudstorageio.tests import resources
from cloudstorageio.configs.configs import CloudInterfaceConfig
from cloudstorageio.enums import PrefixEnums
from cloudstorageio.interface.cloud_interface import CloudInterface
from cloudstorageio.tools.logger import logger


class TestCloudInterface(unittest.TestCase):
    """Tests all CloudInterface methods with all available services"""

    # input all services' prefixes that you want to use
    services = [PrefixEnums.S3, PrefixEnums.DROPBOX, PrefixEnums.GOOGLE_CLOUD, ]
    # services = [PrefixEnums.GOOGLE_DRIVE]

    # IF use S3 or GOOGLE_CLOUD specify bucket names
    S3_BUCKET_NAME = 'test-cloudstorageio'
    GS_BUCKET_NAME = 'test-cloudstorageio'

    # Give credentials manually to CloudInterface or set environment variables with CloudInterfaceConfig
    config_path = os.environ.get("CI_CONFIG_PATH")
    CloudInterfaceConfig.set_configs(config_json_path=config_path)

    ci = CloudInterface()

    # local files path (should be unchangeable)
    MOON_FILE = 'moon.jpg'
    TEST_FOLDER = 'TEST_CI'
    SAMPLE_FOLDER = 'sample_files'
    LOREM_FILE = 'lorem.txt'
    APPLE_FILE = 'apple.jpg'

    resources_folder_path = os.path.abspath(os.path.join(os.path.dirname(resources.__file__)))
    local_test_folder = os.path.join(resources_folder_path, TEST_FOLDER)
    test_folder_path = None

    @classmethod
    def get_random_service_path(cls):
        """Selects random interface from given services' list"""

        interface = random.choice(cls.services)

        # remove chosen service for variety
        if len(cls.services) > 1:
            cls.services.remove(interface)
        logger.info(f"Testing {interface.value} interface")
        if interface == PrefixEnums.GOOGLE_CLOUD:
            test_folder_path = interface.value + os.path.join(cls.GS_BUCKET_NAME, cls.TEST_FOLDER)
        elif interface == PrefixEnums.S3:
            test_folder_path = interface.value + os.path.join(cls.S3_BUCKET_NAME, cls.TEST_FOLDER)
        else:
            test_folder_path = interface.value + cls.TEST_FOLDER

        return test_folder_path

    @classmethod
    def setUpClass(cls) -> None:
        """Chooses random interface and copies local TEST_CI folder into chosen storage"""

        warnings.filterwarnings(action="ignore", message="unclosed",
                                category=ResourceWarning)

        cls.test_folder_path = cls.get_random_service_path()
        # copies local TEST_CI folder to remote storage

        cls.ci.copy_dir(source_dir=cls.local_test_folder, dest_dir=cls.test_folder_path, continue_copy=True,
                        multiprocess=False)

    def setUp(self):
        """Sets up some remote and local file/folder paths"""

        self.remote_folder_path = os.path.join(self.test_folder_path, self.SAMPLE_FOLDER)
        self.remote_lorem = os.path.join(self.test_folder_path, self.SAMPLE_FOLDER, self.LOREM_FILE)
        self.not_existing_file = os.path.join(self.test_folder_path, self.MOON_FILE)
        self.remote_apple_file = os.path.join(self.test_folder_path, self.APPLE_FILE)

        self.local_moon_files = os.path.join(self.resources_folder_path, self.MOON_FILE)

    def test_identify_path_type(self):
        """Tries invalid path """
        invalid_prefix = 'dx:/'
        invalid_path = invalid_prefix + self.APPLE_FILE

        self.assertRaises(ValueError, self.ci.identify_path_type, invalid_path)

    def test_fetch(self):
        """Tests for fetch and open(the same fetch logic) methods"""

        # Tries to fetch(read) not existing file
        self.assertRaises(FileNotFoundError, self.ci.fetch, self.not_existing_file)

        # Reads content with context manager, reads as string (mode = 'r')
        with self.ci.open(file_path=self.remote_lorem, mode='r') as f:
            res = f.read()
        self.assertIsInstance(res, str)

    def test_save_and_remove(self):
        """Tests save, listdir and remove"""

        state1 = self.ci.listdir(path=self.test_folder_path)

        # writes local file content to remote storage
        content = self.ci.fetch(self.local_moon_files)
        self.ci.save(path=self.not_existing_file, content=content)

        state2 = self.ci.listdir(path=self.test_folder_path)

        self.assertEqual(len(state1), (len(state2)-1))

        # Removes added file
        self.ci.remove(self.not_existing_file)

        state3 = self.ci.listdir(path=self.test_folder_path)
        self.assertEqual(len(state1), (len(state3)))

    def test_isfile(self):
        """Test isfile method"""

        # Test on existing file (in storage)
        res = self.ci.isfile(path=self.remote_lorem)
        self.assertEqual(res, True)

        # Test on not existing file
        res = self.ci.isfile(path=self.not_existing_file)
        self.assertEqual(res, False)

    def test_isdir(self):
        """Test isdir method"""
        # Test on file (not folder)
        res = self.ci.isdir(path=self.remote_apple_file)
        self.assertEqual(res, False)

        # Test on existing folder
        res = self.ci.isdir(path=self.remote_folder_path)
        self.assertEqual(res, True)

    def test_listdir(self):
        """ Tests listdir method with various params"""
        # Tests recursive param
        res1 = self.ci.listdir(path=self.test_folder_path, recursive=True)
        res2 = self.ci.listdir(path=self.test_folder_path, recursive=False)
        self.assertEqual(len(res1), (len(res2) + 1))

        # Tests exclude folder (can be also recursive=False)
        res1 = self.ci.listdir(path=self.test_folder_path, recursive=True, exclude_folders=False)
        res2 = self.ci.listdir(path=self.test_folder_path, recursive=True, exclude_folders=True)
        self.assertEqual(len(res1), (len(res2) + 1))

    def test_copy_and_move(self):
        """Tests copy and move"""

        new_interface = self.get_random_service_path()
        new_file_path = os.path.join(new_interface, self.APPLE_FILE)

        self.ci.copy(from_path=self.remote_apple_file, to_path=new_file_path)
        self.assertEqual(self.ci.isfile(new_file_path), True)
        # could be tha same service
        if new_file_path != self.test_folder_path:
            # cleans new service's storage, and checks overwriting case in our service
            self.ci.move(from_path=new_file_path, to_path=self.remote_apple_file)
            self.assertEqual(self.ci.isfile(new_file_path), False)

    def test_copy_batch(self):
        """Tests copy batch and folder remove"""
        new_interface = self.get_random_service_path()
        new_folder_path = os.path.join(new_interface, self.SAMPLE_FOLDER)
        self.ci.copy_dir(source_dir=self.remote_folder_path, dest_dir=new_folder_path)

        self.assertEqual(self.ci.isdir(new_folder_path), True)

        if new_folder_path != self.remote_folder_path:
            # cleans new service's storage
            self.ci.remove(path=new_folder_path)
            self.assertEqual(self.ci.isdir(new_folder_path), False)

    @classmethod
    def tearDownClass(cls) -> None:
        """Removes TEST_CI folder from remote storage"""
        cls.ci.remove(path=cls.test_folder_path)
