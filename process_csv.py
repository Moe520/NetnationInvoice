import pandas as pd
from pandas.io.parsers import ParserError
import json
import os
import argparse
import logging

from utils.input_validation_utils import get_terminal_args

from skippable_row_dropper.skippable_row_dropper import SkippableRowDropper
from skippable_row_dropper.strategy.drop_rows_missing_part_no_strategy import DropRowsMissingPartNoStrategy
from skippable_row_dropper.strategy.drop_rows_by_partner_id_strategy import DropRowsByPartnerIdStrategy

from error_logger.error_logger import ErrorLogger

# Default file paths to use if none are given from command line
TYPE_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/typemap.json"
REDUCTION_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/reductionmap.json"

CLEAR_LOGS_ON_STARTUP = True
DEFAULT_ERROR_LOG_FILE_NAME = "csv_error_log.txt"

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

# def insert_statement_3_values(var1, var2, var3):
#   return "INSERT INTO table VALUES (%s, %s, %s)", var1, var2, var3


if __name__ == "__main__":

    csv_error_logger = ErrorLogger(DEFAULT_ERROR_LOG_FILE_NAME)

    if CLEAR_LOGS_ON_STARTUP:
        csv_error_logger.clear_log_file()

    #######################################################################
    # Get the path to the csv (optionally: typemap and reduction map)     #
    #######################################################################

    args = get_terminal_args(TYPE_MAP_DEFAULT_PATH, REDUCTION_MAP_DEFAULT_PATH)

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
        csv_error_logger.warning('ERROR: No json found at, ' + type_map_path)

    ###################################################################
    # Load the reduction map json file                                #
    ###################################################################
    reduction_map = {}
    try:
        with open(reduction_map_path) as f:
            reduction_map = json.load(f)
    except FileNotFoundError:
        csv_error_logger.warning('ERROR: No reduction map json found at, ', reduction_map_path)  # Warn if missing file

    ####################################################################
    # Import the CSV                                                   #
    ####################################################################
    # Try to import the CSV
    try:
        df = pd.read_csv(filepath_or_buffer=args.infile)

    # Give error if file not found
    except FileNotFoundError:
        csv_error_logger.critical_error('ERROR: No CSV found at the specified path')

    # Give error if not valid csv
    except ParserError:
        csv_error_logger.critical_error(csv_file_path, 'Is not a valid csv file')

    ####################################################################
    # Find invalid rows in the CSV and log them                        #
    ####################################################################

    partner_id_row_dropper = SkippableRowDropper(DropRowsByPartnerIdStrategy)
    missing_part_number_row_dropper = SkippableRowDropper(DropRowsMissingPartNoStrategy)

    print("DF beginning")
    print(df.head())

    partner_id_row_dropper.drop_bad_rows(df, csv_error_logger)

    print("DF After partner ID row dropped")
    print(df.head())

    ####################################################################
    # Flag invalid rows in the CSV and log them                        #
    ####################################################################

    # indices_with_invalid_count = df.loc[df["itemCount"] < 1].index

    # log_bulk_indices(indices_with_invalid_count, "Row skipped due to invalid item count: ")
