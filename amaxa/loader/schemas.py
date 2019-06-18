from .. import amaxa, transforms, constants
from .input_type import InputType
import os


def get_available_versions(input_type):
    return SCHEMAS[input_type].keys()


def get_schema(input_type, version):
    return SCHEMAS[input_type][version]


def _env_or_string(params):
    ret = {
        "type": ["string", "dict"],
        "schema": {"env": {"type": "string", "required": True}},
        "coerce": lambda v: v if isinstance(v, str) else os.environ.get(v["env"]),
    }
    ret.update(params)

    return ret


OPTIONS_SCHEMA = {
    "type": "dict",
    "schema": {
        "bulk-api-batch-size": {
            "type": "integer",
            "default": constants.OPTION_DEFAULTS["bulk-api-batch-size"],
            "max": 10000,
            "min": 0
        },
        "bulk-api-timeout": {
            "type": "integer",
            "default": constants.OPTION_DEFAULTS["bulk-api-timeout"],
            "min": 0
        },
        "bulk-api-poll-interval": {
            "type": "integer",
            "default": constants.OPTION_DEFAULTS["bulk-api-poll-interval"],
            "min": 0,
            "max": 60
        }
    }
}

SCHEMAS = {
    InputType.CREDENTIALS: {
        1: {
            "version": {"type": "integer", "required": True, "allowed": [1]},
            "credentials": {
                "type": "dict",
                "required": True,
                "schema": {
                    "username": {
                        "type": "string",
                        "excludes": ["access-token", "instance-url"],
                    },
                    "sandbox": {"type": "boolean", "default": False},
                    "access-token": {
                        "dependencies": ["instance-url"],
                        "type": "string",
                        "excludes": [
                            "username",
                            "password",
                            "security-token",
                            "jwt-key",
                            "jwt-file",
                            "consumer-key",
                        ],
                    },
                    "password": {
                        "dependencies": ["username"],
                        "type": "string",
                        "excludes": [
                            "access-token",
                            "instance-url",
                            "jwt-key",
                            "jwt-file",
                            "consumer-key",
                        ],
                    },
                    "security-token": {
                        "dependencies": ["username", "password"],
                        "type": "string",
                        "excludes": [
                            "access-token",
                            "instance-url",
                            "jwt-key",
                            "jwt-file",
                            "consumer-key",
                        ],
                    },
                    "organization-id": {
                        "dependencies": ["username", "password"],
                        "type": "string",
                        "excludes": [
                            "access-token",
                            "instance-url",
                            "jwt-key",
                            "jwt-file",
                            "consumer-key",
                        ],
                    },
                    "instance-url": {
                        "dependencies": ["access-token"],
                        "type": "string",
                        "excludes": [
                            "username",
                            "password",
                            "security-token",
                            "jwt-key",
                            "jwt-file",
                            "consumer-key",
                        ],
                    },
                    "jwt-key": {
                        "dependencies": ["consumer-key", "username"],
                        "type": "string",
                        "excludes": [
                            "password",
                            "security-token",
                            "access-token",
                            "instance-url",
                            "jwt-file",
                        ],
                    },
                    "jwt-file": {
                        "dependencies": ["consumer-key", "username"],
                        "type": "string",
                        "excludes": [
                            "password",
                            "security-token",
                            "access-token",
                            "instance-url",
                            "jwt-key",
                        ],
                    },
                    "consumer-key": {
                        "dependencies": ["username"],
                        "type": "string",
                        "excludes": [
                            "password",
                            "security-token",
                            "access-token",
                            "instance-url",
                        ],
                    },
                },
            },
        },
        2: {
            "version": {"type": "integer", "required": True, "allowed": [2]},
            "credentials": {
                "type": "dict",
                "required": True,
                "schema": {
                    "sandbox": {"type": "boolean", "default": False},
                    "username": {
                        "type": "dict",
                        "excludes": ["token", "jwt"],
                        "required": True,
                        "schema": {
                            "username": _env_or_string({
                                "required": True,
                            }),
                            "password": _env_or_string({
                                "required": True,
                            }),
                            "security-token": _env_or_string({
                                "required": True,
                                "excludes": "organization-id",
                            }),
                            "organization-id": _env_or_string({
                                "required": True,
                                "excludes": "security-token",
                            }),
                        },
                    },
                    "token": {
                        "type": "dict",
                        "excludes": ["username", "jwt"],
                        "required": True,
                        "schema": {
                            "instance-url": _env_or_string({
                                "required": True,
                            }),
                            "access-token": _env_or_string({
                                "required": True,
                            }),
                        },
                    },
                    "jwt": {
                        "type": "dict",
                        "excludes": ["token", "username"],
                        "required": True,
                        "schema": {
                            "username": _env_or_string({
                                "required": True,
                            }),
                            "keyfile": _env_or_string({
                                "required": True,
                                "excludes": "key",
                            }),
                            "key": _env_or_string({
                                "required": True,
                                "excludes": "keyfile",
                            }),
                            "consumer-key": _env_or_string({
                                "required": True,
                            }),
                        },
                    },
                },
            },
        },
    },
    InputType.LOAD_OPERATION: {
        1: {
            "version": {"type": "integer", "required": True, "allowed": [1]},
            "operation": {
                "type": "list",
                "schema": {
                    "type": "dict",
                    "schema": {
                        "sobject": {"type": "string", "required": True},
                        "file": {
                            "type": "string",
                            "default_setter": lambda doc: doc["sobject"] + ".csv",
                        },
                        "result-file": {
                            "type": "string",
                            "default_setter": lambda doc: doc["sobject"]
                            + "-results.csv",
                        },
                        "input-validation": {
                            "type": "string",
                            "default": "default",
                            "allowed": ["none", "default", "strict"],
                        },
                        "outside-lookup-behavior": {
                            "type": "string",
                            "allowed": amaxa.OutsideLookupBehavior.all_values(),
                            "default": "include",
                        },
                        "self-lookup-behavior": {
                            "type": "string",
                            "allowed": amaxa.SelfLookupBehavior.all_values(),
                            "default": "trace-all",
                        },
                        "extract": {
                            "type": "dict",
                            "schema": {
                                "all": {
                                    "type": "boolean",
                                    "allowed": [True],
                                    "excludes": ["descendents", "query", "ids"],
                                },
                                "descendents": {
                                    "type": "boolean",
                                    "allowed": [True],
                                    "excludes": ["all", "query", "ids"],
                                },
                                "query": {
                                    "type": "string",
                                    "excludes": ["all", "descendents", "ids"],
                                },
                                "ids": {
                                    "type": "list",
                                    "excludes": ["all", "descendents", "query"],
                                    "schema": {"type": "string"},
                                },
                            },
                        },
                        "field-group": {
                            "type": "string",
                            "allowed": ["readable", "writeable", "smart"],
                            "excludes": ["fields"],
                        },
                        "fields": {
                            "type": "list",
                            "excludes": ["field-group"],
                            "schema": {
                                "type": ["string", "dict"],
                                "schema": {
                                    "field": {"type": "string", "required": True},
                                    "column": {"type": "string", "required": False},
                                    "transforms": {
                                        "type": "list",
                                        "schema": {
                                            "type": "string",
                                            "allowed": transforms.__all__,
                                        },
                                        "required": False,
                                    },
                                    "outside-lookup-behavior": {
                                        "type": "string",
                                        "allowed": amaxa.OutsideLookupBehavior.all_values(),
                                    },
                                    "self-lookup-behavior": {
                                        "type": "string",
                                        "allowed": amaxa.SelfLookupBehavior.all_values(),
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        2: {
            "version": {"type": "integer", "required": True, "allowed": [2]},
            "options": OPTIONS_SCHEMA,
            "operation": {
                "type": "list",
                "schema": {
                    "type": "dict",
                    "schema": {
                        "sobject": {"type": "string", "required": True},
                        "options":  OPTIONS_SCHEMA,
                        "file": {
                            "type": "string",
                            "default_setter": lambda doc: doc["sobject"] + ".csv",
                        },
                        "result-file": {
                            "type": "string",
                            "default_setter": lambda doc: doc["sobject"]
                            + "-results.csv",
                        },
                        "input-validation": {
                            "type": "string",
                            "default": "default",
                            "allowed": ["none", "default", "strict"],
                        },
                        "outside-lookup-behavior": {
                            "type": "string",
                            "allowed": amaxa.OutsideLookupBehavior.all_values(),
                            "default": "include",
                        },
                        "self-lookup-behavior": {
                            "type": "string",
                            "allowed": amaxa.SelfLookupBehavior.all_values(),
                            "default": "trace-all",
                        },
                        "extract": {
                            "type": "dict",
                            "schema": {
                                "all": {
                                    "type": "boolean",
                                    "allowed": [True],
                                    "excludes": ["descendents", "query", "ids"],
                                },
                                "descendents": {
                                    "type": "boolean",
                                    "allowed": [True],
                                    "excludes": ["all", "query", "ids"],
                                },
                                "query": {
                                    "type": "string",
                                    "excludes": ["all", "descendents", "ids"],
                                },
                                "ids": {
                                    "type": "list",
                                    "excludes": ["all", "descendents", "query"],
                                    "schema": {"type": "string"},
                                },
                            },
                        },
                        "field-group": {
                            "type": "string",
                            "allowed": ["readable", "writeable", "smart"],
                            "excludes": ["fields"],
                        },
                        "fields": {
                            "type": "list",
                            "excludes": ["field-group"],
                            "schema": {
                                "type": ["string", "dict"],
                                "schema": {
                                    "field": {"type": "string", "required": True},
                                    "column": {"type": "string", "required": False},
                                    "transforms": {
                                        "type": "list",
                                        "schema": {
                                            "type": "string",
                                            "allowed": transforms.__all__,
                                        },
                                        "required": False,
                                    },
                                    "outside-lookup-behavior": {
                                        "type": "string",
                                        "allowed": amaxa.OutsideLookupBehavior.all_values(),
                                    },
                                    "self-lookup-behavior": {
                                        "type": "string",
                                        "allowed": amaxa.SelfLookupBehavior.all_values(),
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
    },
    InputType.STATE: {
        1: {
            "version": {"type": "integer", "required": True, "allowed": [1]},
            "state": {
                "type": "dict",
                "required": True,
                "schema": {
                    "stage": {
                        "type": "string",
                        "required": True,
                        "allowed": amaxa.LoadStage.all_values(),
                    },
                    "id-map": {"type": "dict", "required": True},
                },
            },
        }
    },
}

SCHEMAS[InputType.EXTRACT_OPERATION] = SCHEMAS[InputType.LOAD_OPERATION]
