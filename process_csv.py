import pandas as pd
import numpy as np
from pandas.io.parsers import ParserError
import json
import os

# Modules for input validation, clean up and debugging to console
from utils.input_validation_utils import get_terminal_args
from utils.cleanup_utils import remove_old_outputs
from utils.debug_snapshotter import show_snapshot_if_debugging
from utils.sql_insert_file_outputter import write_inserts_to_text_file

# Strategies for dropping rows that are to be skipped
from skippable_row_dropper.skippable_row_dropper import SkippableRowDropper
from skippable_row_dropper.strategy.drop_rows_missing_part_no_strategy import DropRowsMissingPartNoStrategy
from skippable_row_dropper.strategy.drop_rows_by_partner_id_strategy import DropRowsByPartnerIdStrategy
from skippable_row_dropper.strategy.drop_rows_invalid_item_count_strategy import DropRowsInvalidItemCountStrategy

# Strategies for transformation / mapping of columns
from column_prepper.column_prepper import ColumnPrepper
from column_prepper.strategy.prep_column_remove_non_alphanum_strategy import PrepColumnRemoveNonAlphaNumStrategy
from column_prepper.strategy.prep_column_map_part_number_strategy import PrepColumnMapPartNumberStrategy
from column_prepper.strategy.prep_column_apply_reduction_map_strategy import PrepColumnApplyReductionMapStrategy

# Strategies for parameterizing sql queries
from sql_parameterizer.sql_parameterizer import SqlParameterizer
from sql_parameterizer.strategy.parameterize_sql_chargeable_strategy import ParameterizeSqlChargeableStrategy
from sql_parameterizer.strategy.parameterize_sql_domains_strategy import ParameterizeSqlDomainsStrategy

# Error logger for keeping track of invalid / skippable rows and recording actions to the csv error log
from error_logger.error_logger import ErrorLogger

# Default file paths to use for map files if none are given from command line
TYPE_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/typemap.json"
REDUCTION_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/reductionmap.json"

# Clean up flags
CLEAR_LOGS_ON_STARTUP = True
CLEAR_OLD_OUTPUTS_ON_STARTUP = True
DEFAULT_ERROR_LOG_FILE_NAME = "csv_error_log.txt"

# If on, will see snapshots of the dataframe at each stage of the pipeline
DEBUG_MODE = False

if __name__ == "__main__":

    csv_error_logger = ErrorLogger(DEFAULT_ERROR_LOG_FILE_NAME)  # Error logger to report bad rows to text file

    if CLEAR_LOGS_ON_STARTUP:  # If on, clear the log file from previous runs
        csv_error_logger.clear_log_file()

    if CLEAR_OLD_OUTPUTS_ON_STARTUP:  # If on remove outputs of previous run (otherwise will append)
        remove_old_outputs("domains_sql_insert.txt", "chargeable_sql_insert.txt")

    #######################################################################
    # Get the path to the csv (optionally: typemap and reduction map)     #
    #######################################################################

    # Get the command line args and assign to variables to be validated
    args = get_terminal_args(TYPE_MAP_DEFAULT_PATH, REDUCTION_MAP_DEFAULT_PATH)
    csv_file_path = args.infile
    type_map_path = args.typemap
    reduction_map_path = args.reductionmap

    ###################################################################
    # Load the typemap json file                                      #
    ###################################################################

    type_map = {}
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

    ###############################################################################
    # Find invalid rows in the CSV, log them then drop them                       #
    ###############################################################################

    # Instatiate 3 row droppers , each one with its own strategy
    partner_id_row_dropper = SkippableRowDropper(DropRowsByPartnerIdStrategy)
    missing_part_number_row_dropper = SkippableRowDropper(DropRowsMissingPartNoStrategy)
    bad_item_count_dropper = SkippableRowDropper(DropRowsInvalidItemCountStrategy)

    show_snapshot_if_debugging("DF beginning",df, DEBUG_MODE)

    # Drop Rows whose partner id is on the blacklist in the strategy file
    partner_id_row_dropper.drop_bad_rows(df, csv_error_logger)

    show_snapshot_if_debugging("DF After partner ID row dropped",df,  DEBUG_MODE)

    # Drop rows with missing part numbers
    missing_part_number_row_dropper.drop_bad_rows(df, csv_error_logger)

    show_snapshot_if_debugging("DF After missing part numbers dropped",df,  DEBUG_MODE)

    # Drop rows with non positive item counts
    bad_item_count_dropper.drop_bad_rows(df, csv_error_logger)

    show_snapshot_if_debugging("DF After bad item counts dropped",df,  DEBUG_MODE)

    ###############################################################################
    # Perform Transforms and mappings                                             #
    ###############################################################################

    # Reduce the units based on the map file in maps/reductionmap.json (or the user provided map)
    unit_reduction_transformer = ColumnPrepper(PrepColumnApplyReductionMapStrategy)
    unit_reduction_transformer.prep_column(df, type_map, reduction_map)

    # Use the map in maps/typemap.json to transform the part numbers
    part_num_transformer = ColumnPrepper(PrepColumnMapPartNumberStrategy)
    part_num_transformer.prep_column(df, type_map, reduction_map)
    df = df.rename(columns={'PartNumber': 'PartNumber_mapped'})

    show_snapshot_if_debugging("DF After Part number mapped",df,  DEBUG_MODE)

    # Clean the accountGuid column
    account_guid_transformer = ColumnPrepper(PrepColumnRemoveNonAlphaNumStrategy)
    account_guid_transformer.prep_column(df, type_map, reduction_map)

    show_snapshot_if_debugging("DF After non alphanumerics in account guid removed",df,  DEBUG_MODE)

    ###############################################################################
    # Calculate Summary totals                                                     #
    ###############################################################################

    product_summary = df[["PartNumber_mapped", "itemCount"]].groupby(['PartNumber_mapped']).sum()
    product_summary = product_summary.rename(columns={"itemCount":"Sum(itemCount)"})
    print(product_summary)

    #################################################################################
    # Generate the sql inserts for chargeable table, then drop its data from memory #
    #################################################################################

    # Generate a parameterizer for each target table
    chargeable_sql_parameterizer = SqlParameterizer(ParameterizeSqlChargeableStrategy)
    chargeable_sql_parameterizer.parameterize_df_to_sql(df)

    show_snapshot_if_debugging("DF After chargeables generated",df,  DEBUG_MODE)

    df = df[["accountGuid", "domains", "chargeable_sql_insert"]]

    write_inserts_to_text_file("chargeable_sql_insert.txt",
                               'INSERT INTO chargeable VALUES',
                               df["chargeable_sql_insert"].values)

    df.drop(columns=["chargeable_sql_insert"], inplace=True)

    show_snapshot_if_debugging("DF After chargeable cols dropped",df,  DEBUG_MODE)

    #################################################################################
    # Generate the sql inserts for domains table, then drop its data from memory    #
    #################################################################################

    df = df.drop_duplicates(subset='domains', keep='first')

    domains_sql_parameterizer = SqlParameterizer(ParameterizeSqlDomainsStrategy)
    domains_sql_parameterizer.parameterize_df_to_sql(df)

    show_snapshot_if_debugging("DF After domains duplicates dropped and inserts generated",df,  DEBUG_MODE)

    write_inserts_to_text_file("domains_sql_insert.txt", 'INSERT INTO domains VALUES', df["domains_sql_insert"].values)

    print("Pipeline complete.")
    exit(0)
