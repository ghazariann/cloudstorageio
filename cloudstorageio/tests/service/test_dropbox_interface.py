import os
import unittest

import cloudstorageio
from cloudstorageio.service.dropbox_interface import DropBoxInterface


class TestDropBoxInterface(unittest.TestCase):

    def setUp(self):
        """
        for the beginning make sure to have this structure in your dropbox app

                                                                          sample_files/    v/  sample.jpg  v
                                                                             sample.txt
        """
        self.dbx = DropBoxInterface()

        self.test_folder_path = 'TEST'
        self.dbx_folder_path = 'sample_files'

        self.binary_file = os.path.join(self.test_folder_path, 'sample.jpg')
        self.text_file = os.path.join(self.test_folder_path, 'sample_files/sample.txt')

        # not in storage
        self.not_found_file = os.path.join(self.test_folder_path, 'not_found/not_found.txt')
        self.new_binary_file = os.path.join(self.test_folder_path, 'sample_files/moon.jpg')

        self.sample_text = 'lorem ipsum'
        self.local_test_folder = os.path.abspath(os.path.join(os.path.dirname(cloudstorageio.tests.service.__file__),
                                                              'resources'))
        self.local_pic = os.path.join(self.local_test_folder, 'Moon.jpg')

    def test_read(self):
        # Reading binary file with context manager
        with self.dbx.open(self.binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)

        # Reading text file without 'with' statement
        f = self.dbx.open(self.text_file, 'r')
        res = f.read()
        self.assertIsInstance(res, str)

        open_state = self.dbx.open(self.not_found_file)
        self.assertRaises(FileNotFoundError, open_state.read)

    def test_write(self):
        # Writing text file without context manager
        f = self.dbx.open(self.text_file)
        f.write(self.sample_text)

        with self.dbx.open(self.text_file, 'r') as f:
            output = f.read()
        self.assertEqual(output, self.sample_text)

        # Writing binary file with context manager
        with open(self.local_pic, 'rb') as f:
            output = f.read()
        with self.dbx.open(self.new_binary_file, 'w') as f:
            f.write(output)
        with self.dbx.open(self.new_binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)

        # tear down moon.jpg
        self.dbx.remove(self.new_binary_file)

    def test_isfile(self):
        # detecting file even without filename extension
        res1 = self.dbx.isfile(self.text_file)
        self.assertEqual(res1, True)

        # folder path is not a file
        res2 = self.dbx.isfile(self.dbx_folder_path)
        self.assertEqual(res2, False)

    def test_isdir(self):
        # detecting folder (have file with same name)
        res1 = self.dbx.isdir(self.dbx_folder_path)
        self.assertEqual(res1, True)

        # folder path is not a folder
        res2 = self.dbx.isdir(self.binary_file)
        self.assertEqual(res2, False)

    def test_listdir(self):
        res1 = self.dbx.listdir(self.dbx_folder_path)
        self.assertEqual(len(res1), 1)

        self.assertRaises(NotADirectoryError, self.dbx.listdir, self.binary_file)
        self.assertRaises(FileNotFoundError, self.dbx.listdir, self.not_found_file)

    def test_remove(self):
        # remove and compare old and new state's of folder
        res1 = self.dbx.listdir(self.dbx_folder_path)
        self.dbx.remove(self.text_file)
        res2 = self.dbx.listdir(self.dbx_folder_path)
        self.assertEqual(len(res1), len(res2)+1)

        # return state
        self.dbx.open(self.text_file).write(self.sample_text)

    def tearDown(self):
        pass
        # tear down states
