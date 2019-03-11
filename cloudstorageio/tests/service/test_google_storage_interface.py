import os
import unittest

from cloudstorageio.service.google_storage_interface import GoogleStorageInterface


class TestGoogleStorageInterface(unittest.TestCase):

    def setUp(self):
        self.gs = GoogleStorageInterface()
        self.binary_file = 'gs://test-cloudstorageio/sample.jpg'
        self.text_file = 'gs://test-cloudstorageio/sample.txt'
        self.new_binary_file = 'gs://test-cloudstorageio/sample-files/moon.jpg'

        self.sample_text = 'lorem ipsum'
        self.local_test_folder = os.path.join(os.path.dirname(os.getcwd()), 'resources')
        self.local_pic = os.path.join(self.local_test_folder, 'Moon.jpg')

    def test_read(self):
        with self.gs.open(self.binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)

        with self.gs.open(self.text_file, 'r') as f:
            res = f.read()
        self.assertIsInstance(res, str)

    def test_write(self):
        with self.gs.open(self.text_file) as f:
            f.write('lorem ipsum')
        with self.gs.open(self.text_file, 'r') as f:
            output = f.read()
        self.assertEqual(output, self.sample_text)

        with open(self.local_pic, 'rb') as f:
            output = f.read()
        with self.gs.open(self.new_binary_file, 'w') as f:
            f.write(output)
        with self.gs.open(self.new_binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)


