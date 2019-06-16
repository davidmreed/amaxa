from .. import amaxa


class InputType(amaxa.StringEnum):
    CREDENTIALS = "credentials"
    LOAD_OPERATION = "load-operation"
    EXTRACT_OPERATION = "extract-operation"
    STATE = "state"
