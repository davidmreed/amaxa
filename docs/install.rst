Installing Amaxa
---------------------------------------

Installation
************

Amaxa supports Python 3.6.1 or greater.

Prebuilt, single-binary versions of Amaxa (created using ``PyInstaller``) are available in current `releases <https://github.com/davidmreed/amaxa/releases>`_ for Linux, Mac OS X, and Windows 10.

To install Amaxa using ``pip``, execute

.. code-block:: shell

    $ pip install amaxa

Make sure to invoke within a Python 3.6+ virtual environment or specify Python 3.6 or greater as required by your operating system.

Amaxa is operating system-agnostic but is primarily tested on Linux.

Development
***********

To start working with Amaxa in a virtual environment, clone the Git repository. Amaxa's repository is on `GitHub <https://github.com/davidmreed/amaxa>`_, and is mirrored on `GitLab <https://gitlab.com/davidmreed/amaxa>`_.

Then, create a virtual environment for Amaxa and install:

.. code-block:: shell

    $ cd amaxa
    $ python3 -m venv venv
    $ source venv/bin/activate
    $ pip install poetry
    $ poetry install

Amaxa uses ``poetry`` to managed dependencies and ``tox`` with ``pytest`` to execute test runs.

If a valid Salesforce access token and instance URL are present in the environment variables ``INSTANCE_URL`` and ``ACCESS_TOKEN``, integration and end-to-end tests will be run against that Salesforce org; otherwise only unit tests are run. Note that **integration tests are destructive** and require data setup before running. Run integration tests **only** in a Salesforce DX scratch org.

Two scripts are included under ``assets/scripts`` to assist in integration testing. Execute ``prep-scratch-org.sh`` to create a scratch org and make it available to Amaxa integration tests. Execute ``get-auth-params.sh`` if you wish to manually create an org and then expose it to Amaxa; the org's alias must be ``amaxa``.
