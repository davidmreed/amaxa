import itertools
import json
from datetime import datetime, timedelta
from time import sleep
from urllib.parse import urlparse

import salesforce_bulk


def JSONIterator(records):
    def enc(r):
        return json.dumps(r).encode("utf-8")

    yield b"["

    i = iter(records)
    yield enc(next(i))
    for rec in i:
        yield b"," + enc(rec)

    yield b"]"


def BatchIterator(iterator, n=10000):
    while True:
        batch = list(itertools.islice(iterator, n))
        if not batch:
            return

        yield batch


class Connection(object):
    def __init__(self, sf, api_version):
        self._sf = sf
        self._bulk = salesforce_bulk.SalesforceBulk(
            sessionId=self._sf.session_id,
            host=urlparse(self._sf.bulk_url).hostname,
            API_version=api_version,
        )
        self._describe_info = {}
        self._field_maps = {}
        self._key_prefix_map = None

    def get_global_describe(self):
        return self._sf.describe()

    def get_sobject_field_map(self, sobjectname):
        if sobjectname not in self._describe_info:
            self.get_sobject_describe(sobjectname)

        return self._field_maps[sobjectname]

    def get_sobject_describe(self, sobjectname):
        if sobjectname not in self._describe_info:
            self._describe_info[sobjectname] = getattr(self._sf, sobjectname).describe()
            self._field_maps[sobjectname] = {
                f.get("name"): f for f in self._describe_info[sobjectname].get("fields")
            }

        return self._describe_info[sobjectname]

    def get_sobject_name_for_id(self, id):
        if self._key_prefix_map is None:
            global_describe = self.get_global_describe()["sobjects"]
            self._key_prefix_map = {
                sobject["keyPrefix"]: sobject["name"] for sobject in global_describe
            }

        return self._key_prefix_map[id[:3]]

    def _bulk_api_insert_update(
        self,
        job,
        sobject,
        record_list,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
    ):
        batches = []
        for record_batch in BatchIterator(iter(record_list), n=bulk_api_batch_size):
            json_iter = JSONIterator(record_batch)
            batches.append(self._bulk.post_batch(job, json_iter))

        for batch in batches:
            self._bulk.wait_for_batch(
                job,
                batch,
                timeout=bulk_api_timeout,
                sleep_interval=bulk_api_poll_interval,
            )

        self._bulk.close_job(job)

        for batch in batches:
            for r in self._bulk.get_batch_results(batch, job):
                yield r

    def bulk_api_insert(
        self,
        sobject,
        record_list,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
        bulk_api_mode,
    ):
        yield from self._bulk_api_insert_update(
            self._bulk.create_insert_job(
                sobject, contentType="JSON", concurrency=bulk_api_mode
            ),
            sobject,
            iter(record_list),
            bulk_api_timeout,
            bulk_api_poll_interval,
            bulk_api_batch_size,
        )

    def bulk_api_update(
        self,
        sobject,
        record_list,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
        bulk_api_mode,
    ):
        yield from self._bulk_api_insert_update(
            self._bulk.create_update_job(
                sobject, contentType="JSON", concurrency=bulk_api_mode
            ),
            sobject,
            iter(record_list),
            bulk_api_timeout,
            bulk_api_poll_interval,
            bulk_api_batch_size,
        )

    def bulk_api_query(self, sobject, query, date_time_fields, bulk_api_poll_interval):
        job = self._bulk.create_query_job(sobject, contentType="JSON")
        batch = self._bulk.query(job, query)
        self._bulk.close_job(job)

        while not self._bulk.is_batch_done(batch):
            sleep(bulk_api_poll_interval)

        for result in self._bulk.get_all_results_for_query_batch(batch):
            result = json.load(result)
            for rec in result:
                if len(date_time_fields) > 0:
                    # The JSON Bulk API returns DateTime values as epoch seconds,
                    # instead of ISO 8601-format strings.
                    # If we have DateTime fields in our field set, postprocess
                    # the result before we store it.
                    for f in date_time_fields:
                        if rec[f] is not None:
                            # Format the datetime according to Salesforce's
                            # particular wants
                            rec[f] = (
                                datetime.utcfromtimestamp(0)
                                + timedelta(milliseconds=rec[f])
                            ).isoformat(timespec="milliseconds") + "+0000"

                yield rec

    def retrieve_records_by_id(self, sobject, record_ids, field_names):
        for id_batch in BatchIterator(iter(record_ids), n=2000):
            # Make sure Ids are strings
            string_ids = [
                str(each_id) if type(each_id) is not str else each_id
                for each_id in id_batch
            ]
            for r in self._sf.restful(
                "composite/sobjects/{}".format(sobject),
                method="POST",
                data=json.dumps({"ids": string_ids, "fields": list(field_names)}),
            ):
                # None means a record with that Id is not found
                if r is not None:
                    yield r

    def query_records_by_reference_field(self, sobject, field_list, id_field, id_set):
        query = "SELECT {} FROM {} WHERE {} IN ({})"

        max_len = 4000 - len("WHERE {} IN ()".format(id_field))
        max_ids = (
            max_len // 21
        )  # 18 characters plus two quote marks and a comma, per Id

        for id_batch in BatchIterator(iter(id_set), n=max_ids):
            id_string = ",".join(["'{}'".format(str(each_id)) for each_id in id_batch])
            for r in self._sf.query_all(
                query.format(field_list, sobject, id_field, id_string)
            )["records"]:
                yield r
