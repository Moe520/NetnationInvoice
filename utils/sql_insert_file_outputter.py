import numpy as np


def write_inserts_to_text_file(output_path, header_statement, data_ref):
    with open(output_path, "a") as f:
        f.write(header_statement + " \n")
        np.savetxt(f, data_ref, fmt="%s")
