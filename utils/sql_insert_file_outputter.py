import numpy as np


def write_inserts_to_text_file(output_path, header_statement, data_ref):
    """
    Writes sql inserts to a text file
    :param output_path: file to write to
    :param header_statement: The first row of the insert statement
    :param data_ref: parameterized data to write to the file
    :return:
    """
    with open(output_path, "a") as f:
        f.write(header_statement + " \n")
        np.savetxt(f, data_ref, fmt="%s")
