import copy
import json
import os.path

sobject_describes = {}
sobject_list = ["Account", "Contact", "Opportunity", "Task", "Attachment"]

with open(
    os.path.join("assets", "test_describes", "sobjects.json"), "r", encoding="utf-8"
) as d:
    root_describe = json.load(d)

for sobject in sobject_list:
    with open(
        os.path.join("assets", "test_describes", f"{sobject}.json"),
        "r",
        encoding="utf-8",
    ) as d:
        sobject_describes[sobject] = json.load(d)


class MockConnection(object):
    def __init__(
        self,
        bulk_insert_results=[],
        bulk_update_results=[],
        bulk_query_results=[],
        retrieve_results=[],
        query_results=[],
    ):
        self._describe = None
        self._sobject_describes = {}
        self._bulk_insert_results = bulk_insert_results
        self._bulk_update_results = bulk_update_results
        self._bulk_query_results = bulk_query_results
        self._retrieve_results = retrieve_results
        self._query_results = query_results
        self._field_maps = {}

    def get_global_describe(self):
        if self._describe is None:
            self._describe = copy.deepcopy(root_describe)

        return self._describe

    def get_sobject_describe(self, sobject):
        if sobject not in self._sobject_describes:
            self._sobject_describes[sobject] = copy.deepcopy(sobject_describes[sobject])
            self._field_maps[sobject] = {
                f.get("name"): f for f in self._sobject_describes[sobject].get("fields")
            }
        return self._sobject_describes[sobject]

    def get_sobject_field_map(self, sobjectname):
        if sobjectname not in self._sobject_describes:
            self.get_sobject_describe(sobjectname)

        return self._field_maps[sobjectname]

    def bulk_api_insert(
        self,
        sobject,
        record_iterator,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
        bulk_api_mode,
    ):
        for r in self._bulk_insert_results:
            yield r

    def bulk_api_update(
        self,
        sobject,
        record_iterator,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
        bulk_api_mode,
    ):
        for r in self._bulk_update_results:
            yield r

    def bulk_api_query(self, sobject, query, date_time_fields, bulk_api_poll_interval):
        for r in self._bulk_query_results:
            yield r

    def retrieve_records_by_id(self, sobject, record_ids, field_names):
        for r in self._retrieve_results:
            yield r

    def query_records_by_reference_field(self, sobject, field_list, id_field, id_set):
        for r in self._query_results:
            yield r
