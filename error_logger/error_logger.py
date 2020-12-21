import pandas as pd


class ErrorLogger:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path

    def log_to_file_single(self, msg):
        log_ref = open(self.log_file_path, "a")
        log_ref.write(msg)
        log_ref.close()

    def log_to_file_bulk(self, indices_list, pre_msg):
        (pre_msg + pd.Series(indices_list.astype(str))).to_csv(self.log_file_path, mode="a", index=False,
                                                               header=False)

    def clear_log_file(self):
        file_ref = open(self.log_file_path, "w")
        file_ref.close()

    def warning(self, msg):
        print(msg)

    def critical_error(self, msg):
        print(msg)
        exit(1)
