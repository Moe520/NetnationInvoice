from column_prepper.strategy.abstract_column_prep_strategy import ColumnPrepStrategy
import pandas as pd


class PrepColumnMapPartNumberStrategy(ColumnPrepStrategy):

    @staticmethod
    def get_part_name(part_number, type_map):
        """
        :param part_number: part number to find a name for
        :param type_map: tells us what name corresponds to a given part number
        :return: a name for the part if it is found, else the part number itself
        """
        if part_number in type_map:
            return type_map[part_number]
        else:
            return part_number

    def prep_column(self, data_ref, type_map, reduction_map):
        """
        :param data_ref: the dataframe to transform in place
        :param type_map: used to map part number to a name
        :param reduction_map: not used by this strategy
        :return:
        """
        data_ref["PartNumber"] = data_ref["PartNumber"].apply(lambda part_num: self.get_part_name(part_num, type_map))
