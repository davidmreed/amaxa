import io
from .. import amaxa
from unittest.mock import Mock


class MockFileStore(object):
    def __init__(self):
        self.mocks = {}
        self.records = {}
        self.file_mocks = {}

    def get_file(self, sobject, ftype):
        if not (sobject, ftype) in self.file_mocks:
            self.file_mocks[(sobject, ftype)] = io.StringIO()

        return self.file_mocks[(sobject, ftype)]

    def get_csv(self, sobject, ftype):
        if ftype == amaxa.FileType.INPUT and sobject in self.records:
            return self.records[sobject]

        if not (sobject, ftype) in self.mocks:
            self.mocks[(sobject, ftype)] = Mock()

        return self.mocks[(sobject, ftype)]

    def set_csv(self, sobject, ftype, new_csv):
        pass
