import csv
import os
import tempfile
import unittest
import unittest.mock
from collections import defaultdict
from contextlib import contextmanager
from distutils.dir_util import copy_tree

import yaml

import amaxa
import amaxa.api
import amaxa.loader
from amaxa.__main__ import main as main

from .IntegrationTest import IntegrationTest


@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


class test_end_to_end(IntegrationTest):
    def test_round_trip_from_command_line(self):
        with tempfile.TemporaryDirectory() as tempdir:
            copy_tree("assets/test_data_csv", tempdir)
            with cd(tempdir):
                with unittest.mock.patch(
                    "sys.argv",
                    ["amaxa", "--load", "-c", "credentials-env.yml", "test.yml"],
                ):
                    self.assertEqual(0, main())

                # Read the data in so we can validate once it's re-extracted.
                with open("test.yml", "r") as fh:
                    load_op = amaxa.loader.LoadOperationLoader(
                        yaml.safe_load(fh.read()), amaxa.api.Connection(self.connection)
                    )
                    load_op.load()

                    sobject_list = [s.sobjectname for s in load_op.result.steps]

                counts = defaultdict(lambda: 0)
                names = defaultdict(lambda: set())
                for sobject in sobject_list:
                    # For each object, look at the CSV to determine the record count
                    # and the name field(s) we should verify.

                    with open("{}.csv".format(sobject), "r") as csv_file:
                        reader = csv.DictReader(csv_file)
                        for record in reader:
                            counts[sobject] = counts[sobject] + 1
                            if "LastName" in record:
                                names[sobject].add(record["LastName"])
                            elif "Name" in record:
                                names[sobject].add(record["Name"])

                # Re-extract the data.
                with unittest.mock.patch(
                    "sys.argv", ["amaxa", "-c", "credentials-env.yml", "test.yml"]
                ):
                    self.assertEqual(0, main())

                # Validate the results
                for sobject in sobject_list:
                    with open("{}.csv".format(sobject), "r") as csv_file:
                        reader = csv.DictReader(csv_file)
                        for record in reader:
                            self.register_case_record(sobject, record["Id"])

                            counts[sobject] = counts[sobject] - 1
                            if "LastName" in record:
                                names[sobject].remove(record["LastName"])
                            elif "Name" in record:
                                names[sobject].remove(record["Name"])

                    self.assertFalse(names[sobject])
                    self.assertEqual(0, counts[sobject])


class test_end_to_end_transforms(IntegrationTest):
    def test_round_trip_all_transforms(self):
        with tempfile.TemporaryDirectory() as tempdir:
            copy_tree("assets/test_data_transforms", tempdir)
            with cd(tempdir):
                with unittest.mock.patch(
                    "sys.argv",
                    ["amaxa", "--load", "-c", "credentials-env.yml", "test.yml"],
                ):
                    self.assertEqual(0, main())

                result = self.connection.query(
                    "SELECT Id, Name, Description FROM Account WHERE Name = 'Sandia Style Interiors'"
                )
                assert len(result["records"]) == 1
                record = result["records"][0]
                assert record["Name"] == "sandia style interiors"
                assert record["Description"] == "LifestyleLifestyle"

                self.connection.Account.update(
                    record["Id"], {"Name": record["Name"].upper()}
                )

                # Re-extract the data.
                with unittest.mock.patch(
                    "sys.argv", ["amaxa", "-c", "credentials-env.yml", "test.yml"]
                ):
                    self.assertEqual(0, main())

                # Validate the results
                with open("Account.csv", "r") as csv_file:
                    reader = csv.DictReader(csv_file)
                    for record in reader:
                        self.register_case_record("Account", record["Id"])

                        assert record["Name"].islower()
                        assert record["Description"] in [
                            "Lifestyle" * 4,
                            "VC" * 4,
                            "Government" * 4,
                            "Sustainability" * 4,
                        ]
