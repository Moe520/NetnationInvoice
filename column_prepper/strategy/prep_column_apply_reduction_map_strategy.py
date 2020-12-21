from column_prepper.strategy.abstract_column_prep_strategy import ColumnPrepStrategy
import pandas as pd


class PrepColumnApplyReductionMapStrategy(ColumnPrepStrategy):

    @staticmethod
    def apply_unit_reduction(part_number, units, reduction_map):
        if part_number in reduction_map:
            return int(int(units) / int(reduction_map[part_number]))
        else:
            return units

    def prep_column(self, data_ref, type_map, reduction_map):
        data_ref["itemCount"] = data_ref.apply(lambda row: self.apply_unit_reduction(row["PartNumber"],
                                                                                     row["itemCount"],
                                                                                     reduction_map),
                                               axis=1)
