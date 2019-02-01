from .. import amaxa
from unittest.mock import Mock

class MockFileStore(object):
    def __init__(self):
        self.mocks = {}
        self.records = {}

    def get_csv(self, sobject, ftype):
        if ftype == amaxa.FileType.INPUT and sobject in self.records:
            return self.records[sobject]

        if not (sobject, ftype) in self.mocks:
            self.mocks[(sobject, ftype)] = Mock()
        
        return self.mocks[(sobject, ftype)]