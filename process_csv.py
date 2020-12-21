import pandas as pd
from pandas.io.parsers import ParserError
import json
import os
import argparse
import logging

# Default file paths to use if none are given from command line
TYPE_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/typemap.json"
REDUCTION_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/reductionmap.json"

CLEAR_LOGS_ON_STARTUP = True
DEFAULT_ERROR_LOG_FILE_NAME = "csv_error_log.txt"


class CsvLogger:
    def __init__(self, log_file_path="csv_error_log.txt"):
        self.log_file_path = log_file_path

    def clear_log_file(self):
        file_ref = open(self.log_file_path, "w")
        file_ref.close()

    def log_to_file(self, msg):
        log_ref = open(self.log_file_path, "a")
        log_ref.write(msg)
        log_ref.close()


class ConsoleLogger:
    def __init__(self):
        return

    def log_to_stdout(self, msg):
        print("WARNING: " + msg)


'''
invoice_column_map = {
    "PartnerID": int,
    "partnerGuid": str,
    "accountid": str,
    "accountGuid": str,
    "username": str,
    "domains": str,
    "itemname": str,
    "plan": str,
    "itemType": str,
    "PartNumber": str,
    "itemCount": int
}
'''


def log_invalid_item_count_errors(rows):
    return


def insert_statement_3_values(var1, var2, var3):
    return "INSERT INTO table VALUES (%s, %s, %s)", var1, var2, var3


if __name__ == "__main__":

    #######################################################################
    # Get the path to the csv (optionally: typemap and reduction map)     #
    #######################################################################
    csv_logger = CsvLogger(DEFAULT_ERROR_LOG_FILE_NAME)

    if CLEAR_LOGS_ON_STARTUP:
        csv_logger.clear_log_file()

    # Fetch the command line arguments
    parser = argparse.ArgumentParser(description="""
     This script will generate sql insert statements based on a given invoice csv 
    """)

    parser.add_argument("--infile", help="Path to the csv file to be processed",
                        required=True
                        )

    parser.add_argument("--typemap",
                        help="Optional Path to typemap.json file",
                        required=False,
                        default=TYPE_MAP_DEFAULT_PATH
                        )

    parser.add_argument("--reductionmap",
                        help="Optional Path to typemap.json file",
                        required=False,
                        default=REDUCTION_MAP_DEFAULT_PATH
                        )

    args = parser.parse_args()

    csv_file_path = args.infile
    type_map_path = args.typemap
    reduction_map_path = args.reductionmap

    ###################################################################
    # Load the typemap json file                                      #
    ###################################################################

    type_map = {}

    print(type_map_path)

    try:
        with open(type_map_path) as f:
            type_map = json.load(f)
    # Give error if file not found
    except FileNotFoundError:
        logging.warning('ERROR: No json found at, ' + type_map_path)

    ###################################################################
    # Load the reduction map json file                                #
    ###################################################################

    reduction_map = {}

    try:
        with open(reduction_map_path) as f:
            reduction_map = json.load(f)
    # Give error if file not found
    except FileNotFoundError:
        logging.warning('ERROR: No json found at, ', reduction_map_path)

    ####################################################################
    # Import the CSV                                                   #
    ####################################################################
    # Try to import the CSV
    try:
        df = pd.read_csv(
            filepath_or_buffer=args.infile
        )

    # Give error if file not found
    except FileNotFoundError:
        logging.warning('ERROR: No CSV found at the specified path')

    # Give error if not valid csv
    except ParserError:
        logging.warning(csv_file_path, 'Is not a valid csv file')


    ####################################################################
    # Find invalid rows in the CSV and log them                        #
    ####################################################################

    def log_bulk_indices(indices_list, pre_msg):
        (pre_msg + pd.Series(indices_list.astype(str))).to_csv(DEFAULT_ERROR_LOG_FILE_NAME, mode="a", index=False,
                                                               header=False)


    indices_with_missing_part_no = df.loc[pd.isna(df['PartNumber'])].index

    missing_part_no_msg = "Row Skipped due to missing part number: "

    log_bulk_indices(indices_with_missing_part_no, missing_part_no_msg)

    ####################################################################
    # Flag invalid rows in the CSV and log them                        #
    ####################################################################

    indices_with_invalid_count = df.loc[df["itemCount"] < 1].index

    log_bulk_indices(indices_with_invalid_count, "Row skipped due to invalid item count: ")
