import unittest
import json
from functools import reduce
from ..api import BatchIterator, JSONIterator


class test_iterators(unittest.TestCase):
    def test_JSONIterator(self):
        records = ["Test", "Test2"]

        s = reduce(lambda x, y: x + y, JSONIterator(records), b"")

        self.assertEqual(
            b"[" + b",".join([json.dumps(r).encode("utf-8") for r in records]) + b"]", s
        )

    def test_BatchIterator(self):
        l = iter(range(20001))
        b = BatchIterator(l)

        self.assertEqual(10000, len(list(next(b))))
        self.assertEqual(10000, len(list(next(b))))
        self.assertEqual(1, len(list(next(b))))

        with self.assertRaises(StopIteration):
            next(b)
