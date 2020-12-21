from column_prepper.strategy.abstract_column_prep_strategy import ColumnPrepStrategy
import pandas as pd


class PrepColumnMapPartNumberStrategy(ColumnPrepStrategy):

    @staticmethod
    def get_part_name(part_number, type_map):
        print("Part name func called with: " + part_number)
        if part_number in type_map:
            return type_map[part_number]
        else:
            return part_number

    def prep_column(self, data_ref, type_map, reduction_map):
        data_ref["PartNumber"] = data_ref["PartNumber"].apply(lambda part_num: self.get_part_name(part_num, type_map))
