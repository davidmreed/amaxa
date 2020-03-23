import argparse
import logging
import os.path
import sys

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

    # Grab the credential file first. We need it to validate the extraction.
    credential_loader = CredentialLoader(load_file(args.credentials))
    credential_loader.load()

    if credential_loader.errors:
        errors = "\n".join(credential_loader.errors)
        logger.error(f"The supplied credentials were not valid: {errors}")
        return -1

    config = load_file(args.config)

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
        logger.info(f"Input files validated successfully.")
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
        ) as state_file:
            state_file.write(save_state(ex, json_mode))

    return ret


if __name__ == "__main__":
    sys.exit(main())
