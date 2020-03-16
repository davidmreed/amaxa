Supplying Credentials
---------------------

.. note:: Amaxa configuration files are versioned and are validated at runtime. This documentation is for the current version of the credential file, version 2. Upgrade existing projects with version 1 files to make use of new features.

Credentials are supplied in a YAML or JSON file, as shown here (for username and password) authentication.

.. code-block:: yaml

    version: 2
    credentials:
        username:
            username: 'test@example.com'
            password: 'blah'
            security-token: '00000'
            organization-id '00D000000000001'  # This value is optional.
            sandbox: True

Amaxa also allows JWT authentication, for headless operation:

.. code-block:: yaml

    version: 2
    credentials:
        jwt:
            username: 'test@example.com'
            consumer-key: 'GOES_HERE'
            key: |
            -----BEGIN RSA PRIVATE KEY-----
            <snipped>
            -----END RSA PRIVATE KEY-----
            keyfile: server.key

If your JWT key is stored externally in a file, use the key ``keyfile`` with the name of that file rather than including the key inline.

Lastly, if you establish authentication outside Amaxa (with Salesforce DX, for example), you can directly provide an access token and instance URL.

.. code-block:: yaml

    version: 2
    credentials:
        token:
            access-token: '.....'
            instance-url: 'https://test.salesforce.com'

Sourcing Credentials from Environment Variables
***********************************************

Amaxa can draw any credential parameter other than the type of authentication (``username``, ``jwt``, or ``token``) from a user-specified environment variable. Specify this by including a dict with the key ``env`` instead of a literal value for the relevant key. ``env`` entries and literals can be mixed in any combination. For JWT authentication, for example, specify

.. code-block:: yaml

    version: 2
    credentials:
        jwt:
            username:
                env: USERNAME
            consumer-key:
                env: CONSUMER_KEY
            keyfile: server.key

Given this credential file, Amaxa will take its JWT keyfile from ``server.key``, and derive the required user name and consumer key from the environment variables ``USERNAME`` and ``CONSUMER_KEY``.
