import re


def sanitize_string(input_str):
    """
    Removes disallowed characters and patterns to help against sql injection
    :param input_str: the string to be cleaned
    :return: the cleaned string
    """
    danger_patterns = r"( *DROP |;|,| *DELETE| |\n|\r|\r\n)"
    return re.sub(danger_patterns, "", input_str, flags=re.IGNORECASE)