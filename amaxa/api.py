import salesforce_bulk
from urllib.parse import urlparse


class Connection(object):
    def __init__(self, sf):
        self._sf = sf
        self._bulk = salesforce_bulk.SalesforceBulk(
            sessionId=self.connection.session_id,
            host=urlparse(self.connection.bulk_url).hostname,
        )

    def get_global_describe(self):
        return self._sf.describe()

    def get_sobject_describe(self, sobject):
        return getattr(self._sf, sobject).describe()

    def _bulk_api_insert_update(
        self,
        job,
        sobject,
        record_iterator,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
    ):
        batches = []
        for record_batch in BatchIterator(record_iterator, n=bulk_api_batch_size):
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
            for r in self.context.bulk.get_batch_results(batch, job):
                yield r

    def bulk_api_insert(
        self,
        sobject,
        record_iterator,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
    ):
        yield self._bulk_api_insert_update(
            self._bulk.create_insert_job(self.sobjectname, contentType="JSON"),
            sobject,
            record_iterator,
            bulk_api_timeout,
            bulk_api_poll_interval,
            bulk_api_batch_size,
        )

    def bulk_api_update(
        bulk,
        sobject,
        record_iterator,
        bulk_api_timeout,
        bulk_api_poll_interval,
        bulk_api_batch_size,
    ):
        yield _bulk_api_insert_update(
            self._bulk.create_update_job(self.sobjectname, contentType="JSON"),
            sobject,
            record_iterator,
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
                    # The JSON Bulk API returns DateTime values as epoch seconds, instead of ISO 8601-format strings.
                    # If we have DateTime fields in our field set, postprocess the result before we store it.
                    for f in date_time_fields:
                        if rec[f] is not None:
                            # Format the datetime according to Salesforce's particular wants
                            rec[f] = (
                                datetime.utcfromtimestamp(0)
                                + timedelta(milliseconds=rec[f])
                            ).isoformat(timespec="milliseconds") + "+0000"

                yield rec

    def retrieve_records_by_id(self, sobject, record_ids, field_names):
        for id_batch in BatchIterator(iter(record_ids), n=2000):
            for r in self._sf.restful(
                "composite/sobjects/{}".format(sobject),
                method="POST",
                body={"ids": record_ids, "fields": field_names},
            ):
                # None means a record with that Id is not found
                if r is not None:
                    yield r

    def query_records_by_reference_field(self, sobject, field_list, id_field, id_set):
        query = "SELECT {} FROM {} WHERE {} IN ({})"

        ids = id_set.copy()
        max_len = 4000 - len("WHERE {} IN ()".format(id_field))
        max_ids = (
            max_len // 21
        )  # 18 characters plus two quote marks and a comma, per Id

        for id_batch in BatchIterator(iter(id_set), n=max_ids):
            for r in self._sf.query_all(
                query.format(field_list, sobject, id_field, id_batch)
            )["records"]:
                yield r
