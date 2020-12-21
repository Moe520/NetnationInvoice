from skippable_row_dropper.strategy.abstract_drop_rows_strategy import DropRowsStrategy
import pandas as pd


class DropRowsMissingPartNoStrategy(DropRowsStrategy):
    """
    Drops rows without a part number
    """

    def drop_bad_rows(self, data_ref, error_logger):
        """
        :param data_ref: dataframe to drop rows from
        :param error_logger: logger to use when reporting bad rows
        :return:
        """
        indices_with_missing_part_no = data_ref.loc[pd.isna(data_ref['PartNumber'])].index
        error_logger.log_to_file_bulk(indices_with_missing_part_no, "Row Skipped due to missing part number: ")
        data_ref.drop(indices_with_missing_part_no, inplace=True)
