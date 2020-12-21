from skippable_row_dropper.strategy.abstract_drop_rows_strategy import DropRowsStrategy
import pandas as pd


class DropRowsInvalidItemCountStrategy(DropRowsStrategy):
    """
    Drops rows with zero or negative item counts
    """

    def drop_bad_rows(self, data_ref, error_logger):
        """
                :param data_ref: dataframe to drop rows from
                :param error_logger: logger to use when reporting bad rows
                :return:
                """
        indices_with_bad_item_count = data_ref.loc[data_ref['itemCount'] < 1].index
        error_logger.log_to_file_bulk(indices_with_bad_item_count, "Row Skipped due to Bad Item Count: ")
        data_ref.drop(indices_with_bad_item_count, inplace=True)
