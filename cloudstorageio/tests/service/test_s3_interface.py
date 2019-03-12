import os
import unittest

from cloudstorageio.service.s3_interface import S3Interface


class TestS3Interface(unittest.TestCase):

    def setUp(self):
        self.s3 = S3Interface()
        self.binary_file = 's3://test-cloudstorageio/sample.jpg'
        self.text_file = 's3://test-cloudstorageio/sample.txt'
        self.new_binary_file = 's3://test-cloudstorageio/sample-files/moon.txt'

        self.sample_text = 'lorem ipsum'
        self.local_test_folder = os.path.join(os.path.dirname(os.getcwd()), 'resources')
        self.local_pic = os.path.join(self.local_test_folder, 'Moon.jpg')

    def test_read(self):
        # Reading binary file with context manager
        with self.s3.open(self.binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)

        # Reading text file without 'with' statement
        self.s3.open(self.text_file, 'r')
        res = f.read()
        self.assertIsInstance(res, str)

    def test_write(self):
        # Writing text file without context manager
        with self.s3.open(self.text_file) as f:
            f.write('lorem ipsum')
        with self.s3.open(self.text_file, 'r') as f:
            output = f.read()
        self.assertEqual(output, self.sample_text)

        # Writing binary file with context manager
        with open(self.local_pic, 'rb') as f:
            output = f.read()
        with self.s3.open(self.new_binary_file, 'w') as f:
            f.write(output)
        with self.s3.open(self.new_binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)

    def tearDown(self):
        pass
