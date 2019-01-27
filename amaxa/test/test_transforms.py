import unittest
from .. import transforms


class test_transforms(unittest.TestCase):
    def test_transforms(self):
        self.assertEqual('test', transforms.strip('  test  '))
        self.assertEqual('test', transforms.lowercase('TEst'))
        self.assertEqual('TEST', transforms.uppercase('tesT'))