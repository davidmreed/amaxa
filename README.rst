Amaxa - a multi-object ETL tool for Salesforce
==============================================

Introduction
------------

Amaxa is a new data loader and ETL (extract-transform-load) tool for Salesforce, designed to support the extraction and loading of complex networks of records in a single operation. For example, an Amaxa operation can extract a designated set of Accounts, their associated Contacts and Opportunities, their Opportunity Contact Roles, and associated Campaigns, and then load all of that data into another Salesforce org while preserving the connections between the records.

Amaxa is designed to replace complex, error-prone workflows that manipulate data exports with spreadsheet functions like ``VLOOKUP()`` to maintain object relationships.

Amaxa is free and open source software, distributed under the BSD License. Amaxa is by `David Reed <https://ktema.org>`_, (c) 2019-2020.

Documentation on `ReadTheDocs <https://amaxa.readthedocs.io>`_ covers the development version of Amaxa (as found in the ``master`` branch). For documentation that covers the latest versions of Amaxa available on PyPI, please view the README found on the `PyPI listing <https://pypi.org/project/amaxa/>`_.

What Does Amaxa Mean?
---------------------

`ἄμαξα <http://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.04.0058%3Aentry%3Da\)%2Fmaca>`_ is the Ancient Greek word for a wagon.
