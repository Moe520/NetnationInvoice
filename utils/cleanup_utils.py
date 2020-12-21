import os


def remove_old_outputs(chargeable_path, domains_path):
    """
    Remove the given sql insert text files if they exist

    :param chargeable_path: Path to the chargeable table sql inserts
    :param domains_path: Path to the domains table sql inserts
    :return:
    """
    try:
        os.remove(chargeable_path)
    except OSError:
        pass

    try:
        os.remove(domains_path)
    except OSError:
        pass
