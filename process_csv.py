import pandas as pd
import numpy as np
from pandas.io.parsers import ParserError
import json
import os

from utils.input_validation_utils import get_terminal_args

from skippable_row_dropper.skippable_row_dropper import SkippableRowDropper
from skippable_row_dropper.strategy.drop_rows_missing_part_no_strategy import DropRowsMissingPartNoStrategy
from skippable_row_dropper.strategy.drop_rows_by_partner_id_strategy import DropRowsByPartnerIdStrategy
from skippable_row_dropper.strategy.drop_rows_invalid_item_count_strategy import DropRowsInvalidItemCountStrategy

from column_prepper.column_prepper import ColumnPrepper
from column_prepper.strategy.prep_column_remove_hyphen_strategy import PrepColumnRemoveHyphensStrategy
from column_prepper.strategy.prep_column_map_part_number_strategy import PrepColumnMapPartNumberStrategy
from column_prepper.strategy.prep_column_apply_reduction_map_strategy import PrepColumnApplyReductionMapStrategy

from sql_parameterizer.sql_parameterizer import SqlParameterizer
from sql_parameterizer.strategy.parameterize_sql_chargeable_strategy import ParameterizeSqlChargeableStrategy
from sql_parameterizer.strategy.parameterize_sql_domains_strategy import ParameterizeSqlDomainsStrategy

from error_logger.error_logger import ErrorLogger

# Default file paths to use if none are given from command line
TYPE_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/typemap.json"
REDUCTION_MAP_DEFAULT_PATH = os.path.dirname(os.path.abspath(__file__)) + "/maps/reductionmap.json"

CLEAR_LOGS_ON_STARTUP = True
DEFAULT_ERROR_LOG_FILE_NAME = "csv_error_log.txt"

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

    ###############################################################################
    # Find invalid rows in the CSV, log them then drop them                       #
    ###############################################################################

    partner_id_row_dropper = SkippableRowDropper(DropRowsByPartnerIdStrategy)
    missing_part_number_row_dropper = SkippableRowDropper(DropRowsMissingPartNoStrategy)
    bad_item_count_dropper = SkippableRowDropper(DropRowsInvalidItemCountStrategy)

    print("DF beginning")
    print(df.head())

    partner_id_row_dropper.drop_bad_rows(df, csv_error_logger)

    print("DF After partner ID row dropped")
    print(df.head())

    missing_part_number_row_dropper.drop_bad_rows(df, csv_error_logger)

    print("DF After missing part numbers dropped")
    print(df.head())

    bad_item_count_dropper.drop_bad_rows(df, csv_error_logger)

    print("DF After bad item counts dropped")
    print(df.head())

    unit_reduction_transformer = ColumnPrepper(PrepColumnApplyReductionMapStrategy)
    unit_reduction_transformer.prep_column(df, type_map, reduction_map)

    print("DF After Units Reduced")
    print(df.head())
    print(df[df["PartNumber"] == "PMQ00005GB0R"])

    part_num_transformer = ColumnPrepper(PrepColumnMapPartNumberStrategy)
    part_num_transformer.prep_column(df, type_map, reduction_map)
    df = df.rename(columns={'PartNumber': 'PartNumber_mapped'})

    print("DF After Part number mapped")
    print(df.head())

    account_guid_transformer = ColumnPrepper(PrepColumnRemoveHyphensStrategy)
    account_guid_transformer.prep_column(df, type_map, reduction_map)

    print("DF After non alphanumerics in account guid removed")
    print(df.head())

    chargeable_sql_parameterizer = SqlParameterizer(ParameterizeSqlChargeableStrategy)
    chargeable_sql_parameterizer.parameterize_df_to_sql(df)

    print("DF After chargeables generated")
    print(df.head())

    df = df[["accountGuid", "domains", "chargeable_sql_insert"]]

    with open("chargeable_sql_insert.txt", "a") as f:
        f.write('INSERT INTO chargeable VALUES \n ')
        np.savetxt(f, df["chargeable_sql_insert"].values, fmt="%s")

    df.drop(columns=["chargeable_sql_insert"], inplace=True)

    print("DF After chargeable cols dropped")
    print(df.head())

    df = df.drop_duplicates(subset='domains', keep='first')

    domains_sql_parameterizer = SqlParameterizer(ParameterizeSqlDomainsStrategy)
    domains_sql_parameterizer.parameterize_df_to_sql(df)

    print("DF After domains duplicates dropped and inserts generated")
    print(df.head())

    with open("domains_sql_insert.txt", "a") as f:
        f.write('INSERT INTO domains VALUES \n ')
        np.savetxt(f, df["domains_sql_insert"].values, fmt="%s")

    print("Pipeline complete")
    exit(0)



