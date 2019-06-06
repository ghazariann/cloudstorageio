import os
import unittest

from cloudstorageio.service.google_storage_interface import GoogleStorageInterface


class TestGoogleStorageInterface(unittest.TestCase):

    def setUp(self):
        """
        for the beginning make sure to have this structure in your google cloud  storage

                                                                          sample-files/    v/  sample.jpg  v
                                                                             sample.txt
        """
        self.gs = GoogleStorageInterface()

        # this 4 paths have to be in storage
        self.folder_path = 'gs://test-cloudstorageio/sample-files'

        # folder and file with the same name
        self.file_and_folder = 'gs://test-cloudstorageio/v'
        self.binary_file = 'gs://test-cloudstorageio/sample.jpg'
        self.text_file = 'gs://test-cloudstorageio/sample-files/sample.txt'

        # not in storage
        self.not_found_file = 'gs://test-cloudstorageio/not_found/not_found.txt'
        self.new_binary_file = 'gs://test-cloudstorageio/sample-files/moon.jpg'
        self.sample_text = 'lorem ipsum'
        self.local_test_folder = os.path.join(os.path.dirname(os.getcwd()), 'resources')
        self.local_pic = os.path.join(self.local_test_folder, 'Moon.jpg')

    def test_read(self):
        # Reading binary file with context manager
        with self.gs.open(self.binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)

        # Reading text file without 'with' statement
        f = self.gs.open(self.text_file, 'r')
        res = f.read()
        self.assertIsInstance(res, str)
        open_state = self.gs.open(self.not_found_file)
        self.assertRaises(FileNotFoundError, open_state.read)

    def test_write(self):
        # Writing text file without context manager
        f = self.gs.open(self.text_file)
        f.write(self.sample_text)
        with self.gs.open(self.text_file, 'r') as f:
            output = f.read()
        self.assertEqual(output, self.sample_text)

        # Writing binary file with context manager
        with open(self.local_pic, 'rb') as f:
            output = f.read()
        with self.gs.open(self.new_binary_file, 'w') as f:
            f.write(output)
        with self.gs.open(self.new_binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)

    def test_isfile(self):
        # detecting file even without filename extension
        res1 = self.gs.isfile(self.file_and_folder)
        self.assertEqual(res1, True)

        # folder path is not a file
        res2 = self.gs.isfile(self.folder_path)
        self.assertEqual(res2, False)

    def test_isdir(self):
        # detecting folder (have file with same name)
        res1 = self.gs.isdir(self.file_and_folder)
        self.assertEqual(res1, True)

        # folder path is not a folder
        res2 = self.gs.isdir(self.binary_file)
        self.assertEqual(res2, False)

    def test_listdir(self):
        res1 = self.gs.listdir(self.file_and_folder)
        self.assertEqual(len(res1), 1)

        self.assertRaises(NotADirectoryError, self.gs.listdir, self.binary_file)
        self.assertRaises(FileNotFoundError, self.gs.listdir, self.not_found_file)

    def test_remove(self):
        # remove and compare old and new state's of folder
        res1 = self.gs.listdir(self.folder_path)
        self.gs.remove(self.text_file)
        res2 = self.gs.listdir(self.folder_path)
        self.assertEqual(len(res1), len(res2)+1)

        # return state
        self.gs.open(self.text_file).write(self.sample_text)

    def tearDown(self):
        pass
        # tear down states
        # try:
        #     self.s3.remove(self.new_binary_file)
        #     self.s3.open(self.text_file).write(self.sample_text)
        # except Exception:
        #     pass
