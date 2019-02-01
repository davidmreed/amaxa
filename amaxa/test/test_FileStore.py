import unittest
from unittest.mock import Mock
from .. import amaxa

class test_FileStore(unittest.TestCase):
    def test_FileStore_tracks_file_handles(self):
        fs = amaxa.FileStore()

        f = Mock()
        g = Mock()
        fs.set_file('Account', amaxa.FileType.INPUT, f)
        fs.set_file('Account', amaxa.FileType.OUTPUT, g)

        self.assertEqual(f, fs.get_file('Account', amaxa.FileType.INPUT))
        self.assertEqual(g, fs.get_file('Account', amaxa.FileType.OUTPUT))
    
    def test_FileStore_tracks_csvs(self):
        fs = amaxa.FileStore()

        f = Mock()
        g = Mock()
        fs.set_csv('Account', amaxa.FileType.INPUT, f)
        fs.set_csv('Account', amaxa.FileType.OUTPUT, g)

        self.assertEqual(f, fs.get_csv('Account', amaxa.FileType.INPUT))
        self.assertEqual(g, fs.get_csv('Account', amaxa.FileType.OUTPUT))

    def test_FileStore_closes_files(self):
        fs = amaxa.FileStore()

        f = Mock()
        g = Mock()
        h = Mock()
        fs.set_file('Account', amaxa.FileType.INPUT, f)
        fs.set_file('Account', amaxa.FileType.OUTPUT, g)
        fs.set_csv('Account', amaxa.FileType.OUTPUT, h)

        fs.close()

        f.close.assert_called_once_with()
        g.close.assert_called_once_with()
        h.close.assert_not_called()