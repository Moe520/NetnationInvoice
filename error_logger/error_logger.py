import pandas as pd


class ErrorLogger:
    """
    Logs info about bad rows to file. Will instantiate with an output path and use that path to append outputs
    """
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path

    def log_to_file_single(self, msg):
        """
        :param msg: Writes a single line to the csv_error_log
        :return:
        """
        log_ref = open(self.log_file_path, "a")
        log_ref.write(msg)
        log_ref.close()

    def log_to_file_bulk(self, indices_list, pre_msg):
        """
        :param msg: Writes a whoe dataframe to the csv_error_log
        :return:
        """
        (pre_msg + pd.Series(indices_list.astype(str))).to_csv(self.log_file_path, mode="a", index=False,
                                                               header=False)

    def clear_log_file(self):
        """
        Empties the log file
        :return:
        """
        file_ref = open(self.log_file_path, "w")
        file_ref.close()

    def warning(self, msg):
        """
        Sends a message to stdout
        :param msg: message to send
        :return:
        """
        print(msg)

    def critical_error(self, msg):
        """
        Prints a message then exits with error code
        :param msg: message to send
        :return:
        """
        print(msg)
        exit(1)
