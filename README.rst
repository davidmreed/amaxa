Amaxa - a multi-object ETL tool for Salesforce
==============================================

.. image:: https://github.com/davidmreed/amaxa/workflows/Feature%20Tests/badge.svg?branch=master
  :target: https://github.com/davidmreed/amaxa
  :alt: Unit Tests

.. image:: https://github.com/davidmreed/amaxa/workflows/Integration%20Test/badge.svg?branch=master
  :target: https://github.com/davidmreed/amaxa
  :alt: Integration Tests

.. image:: https://codecov.io/gh/davidmreed/amaxa/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/davidmreed/amaxa
  :alt: Code Coverage

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
  :target: https://github.com/psf/black
  :alt: Black Code Formatting

.. image:: https://img.shields.io/pypi/l/amaxa
  :alt: PyPI - License

.. image:: https://readthedocs.org/projects/amaxa/badge/?version=latest
  :target: https://amaxa.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status


Introduction
------------

Amaxa is a new data loader and ETL (extract-transform-load) tool for Salesforce, designed to support the extraction and loading of complex networks of records in a single operation. For example, an Amaxa operation can extract a designated set of Accounts, their associated Contacts and Opportunities, their Opportunity Contact Roles, and associated Campaigns, and then load all of that data into another Salesforce org while preserving the connections between the records.

Amaxa is designed to replace complex, error-prone workflows that manipulate data exports with spreadsheet functions like ``VLOOKUP()`` to maintain object relationships.

Amaxa is free and open source software, distributed under the BSD License. Amaxa is by `David Reed <https://ktema.org>`_, (c) 2019-2020.

Documentation for Amaxa is available on `ReadTheDocs <https://amaxa.readthedocs.io>`_. The project is developed on `GitHub <https://github.com/davidmreed/amaxa>`_.

What Does Amaxa Mean?
---------------------

`ἄμαξα <http://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.04.0058%3Aentry%3Da\)%2Fmaca>`_ is the Ancient Greek word for a wagon.
