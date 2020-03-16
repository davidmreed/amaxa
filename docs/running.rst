Running Amaxa
-------------

The command-line API is very simple. To extract data, given an operation definition file ``op.yml`` and a credential file ``cred.yml``, run

.. code-block:: shell

    $ amaxa --credentials cred.yml op.yml

To perform the load of the same operation definition, just add ``--load``:

.. code-block:: shell

    $ amaxa --credentials cred.yml op.yml --load

Operation definitions are generally built to support both load and extract of the same object network. For details, see below. While the examples in this guide are in YAML format, Amaxa supports JSON at feature parity and with the same schemas.

The only other command-line switch provided by Amaxa is ``--verbosity``. Supported levels are ``quiet``, ``errors``, ``normal``, and ``verbose``, in ascending order of verbosity.

To see usage help, execute

.. code-block:: shell

    $ amaxa --help
