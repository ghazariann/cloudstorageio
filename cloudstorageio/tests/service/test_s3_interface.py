import unittest

from cloudstorageio.service.s3_interface import S3Interface


class TestS3Interface(unittest.TestCase):

    def setUp(self):
        self.s3 = S3Interface()
        self.binary_file = 's3://test-cloudstorageio/sample.jpg'
        self.text_file = 's3://test-cloudstorageio/sample.txt'

    def test_read(self):
        with self.s3.open(self.binary_file, 'rb') as f:
            res = f.read()
        self.assertIsInstance(res, bytes)
        with self.s3.open(self.text_file, 'r') as f:
            res = f.read()
        self.assertIsInstance(res, str)
