import os

def remove_old_outputs(chargeable_path,domains_path):
    os.remove(chargeable_path)
    os.remove(domains_path)