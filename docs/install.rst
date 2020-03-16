Installing Amaxa
---------------------------------------

Installation
************

Amaxa requires Python 3.7 or 3.8.

To install Amaxa using ``pip``, execute

.. code-block:: shell

    $ pip install amaxa

Make sure to invoke within a Python 3.7+ virtual environment or specify Python 3.7 or greater as required by your operating system.

Amaxa is operating system-agnostic. It has been tested primarily on Linux. Amaxa has been known to work in some Windows environments (such as MINGW), but is not actively tested on Windows.

Development
***********

To start working with Amaxa in a virtual environment, clone the Git repository. Amaxa's primary repository is on `GitLab <https://gitlab.com/davidmreed/amaxa>`_, and is mirrored on GitHub.

Then, create a virtual environment for Amaxa and install:

.. code-block:: shell

    $ cd amaxa
    $ python3 -m venv venv
    $ source venv/bin/activate
    $ pip install poetry
    $ poetry install

Amaxa uses ``poetry`` to managed dependencies and ``tox`` with ``pytest`` to execute test runs.

If a valid Salesforce access token and instance URL are present in the environment variables ``INSTANCE_URL`` and ``ACCESS_TOKEN``, integration and end-to-end tests will be run against that Salesforce org; otherwise only unit tests are run. Note that **integration tests are destructive** and require data setup before running. Run integration tests **only** in a Salesforce DX scratch org (see ``.gitlab-ci.yml`` for the specific testing process).

Two scripts are included under ``assets/scripts`` to assist in integration testing. Execute ``prep-scratch-org.sh`` to create a scratch org and make it available to Amaxa integration tests. Execute ``get-auth-params.sh`` if you wish to manually create an org and then expose it to Amaxa; the org's alias must be ``amaxa``.
