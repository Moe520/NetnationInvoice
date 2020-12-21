import os


def remove_old_outputs(chargeable_path, domains_path):
    try:
        os.remove(chargeable_path)
    except OSError:
        pass

    try:
        os.remove(domains_path)
    except OSError:
        pass
