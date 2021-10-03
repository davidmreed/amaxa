import argparse
import logging
import os.path
import sys

from amaxa import constants
from amaxa.loader import (
    CredentialLoader,
    ExtractionOperationLoader,
    LoadOperationLoader,
    StateLoader,
    load_file,
    save_state,
)


def main():
    a = argparse.ArgumentParser()

    a.add_argument("config", type=argparse.FileType("r"))
    a.add_argument(
        "-c",
        "--credentials",
        required=True,
        dest="credentials",
        type=argparse.FileType("r"),
    )
    a.add_argument("-l", "--load", action="store_true")
    a.add_argument("-s", "--use-state", dest="use_state", type=argparse.FileType("r"))
    a.add_argument("-k", "--check-only", dest="check_only", action="store_true")
    verbosity_levels = {
        "quiet": logging.NOTSET,
        "errors": logging.ERROR,
        "normal": logging.INFO,
        "verbose": logging.DEBUG,
    }

    a.add_argument(
        "-v",
        "--verbosity",
        choices=verbosity_levels.keys(),
        dest="verbosity",
        default="normal",
        help="Log all actions",
    )

    args = a.parse_args()

    logger = logging.getLogger("amaxa")

    logger.setLevel(verbosity_levels[args.verbosity])
    logger.handlers[:] = [logging.StreamHandler()]

    # Before loading anything, we need the desired API version, which is set
    # as part of the operation definition.

    # Obtain it before formally loading the config, since loading the config
    # requires a Connection, which requires the API version.
    # This solution is not ideal.
    config = load_file(args.config)
    if (
        "options" in config
        and type(config["options"]) == dict
        and "api-version" in config["options"]
    ):
        api_version = config["options"]["api-version"]
        if not (
            type(api_version) == str
            and len(api_version) == 4
            and api_version[2:] == ".0"
            and api_version[:2].isdigit()
        ):
            logger.error(f"API version {api_version} is not valid.")
            return -1
    else:
        api_version = constants.OPTION_DEFAULTS["api-version"]

    # Grab the credential file first. We need it to validate the extraction.
    credential_loader = CredentialLoader(load_file(args.credentials), api_version)
    credential_loader.load()

    if credential_loader.errors:
        errors = "\n".join(credential_loader.errors)
        logger.error(f"The supplied credentials were not valid: {errors}")
        return -1

    if args.load:
        operation_loader = LoadOperationLoader(
            config, credential_loader.result, use_state=args.use_state is not None
        )
    else:
        operation_loader = ExtractionOperationLoader(config, credential_loader.result)

    operation_loader.load()
    if operation_loader.errors:
        errors = "\n".join(operation_loader.errors)
        logger.error(f"Errors occured during load of the operation: {errors}")
        return -1

    ex = operation_loader.result

    if args.use_state:
        state_file = load_file(args.use_state)
        state_loader = StateLoader(state_file, ex)
        state_loader.load()
        if state_loader.errors:
            errors = "\n".join(state_loader.errors)
            logger.error(f"Errors occured during load of the state file: {errors}")
            return -1

    if args.check_only:
        logger.info("Input files validated successfully.")
        return 0

    ret = ex.run()

    if args.load and ret != 0 and ex.global_id_map:
        # Save the operation state.
        json_mode = args.config.name.endswith("json")
        with open(
            os.path.splitext(args.config.name)[0]
            + ".state"
            + (".json" if json_mode else ".yaml"),
            "w",
            encoding="utf-8",
        ) as state_file:
            state_file.write(save_state(ex, json_mode))

    return ret


if __name__ == "__main__":
    sys.exit(main())
