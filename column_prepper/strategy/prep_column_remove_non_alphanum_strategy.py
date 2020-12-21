from column_prepper.strategy.abstract_column_prep_strategy import ColumnPrepStrategy
import pandas as pd


class PrepColumnRemoveNonAlphaNumStrategy(ColumnPrepStrategy):

    def prep_column(self, data_ref, type_map, reduction_map):
        data_ref["accountGuid"] = data_ref["accountGuid"].str.replace('\W', '')
