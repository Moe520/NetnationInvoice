from column_prepper.strategy.abstract_column_prep_strategy import ColumnPrepStrategy
import pandas as pd


class PrepColumnApplyReductionMapStrategy(ColumnPrepStrategy):
    """
        Transforms the item count column in place by reducing it according to map/reductionmap.json
    """
    @staticmethod
    def apply_unit_reduction(part_number, units, reduction_map):
        """
        :param part_number: The part number to be used for the map lookup
        :param units: Initial units (may be reduced or returned as is)
        :param reduction_map: (the reduction map to be used to reduce the unit count)
        :return: a reduced item count if the part number is in the lookup, else return item count as is
        """
        if part_number in reduction_map:
            return int(int(units) / int(reduction_map[part_number]))
        else:
            return units

    def prep_column(self, data_ref, type_map, reduction_map):
        """

        :param data_ref: name referring to the dataframe to be acted upon
        :param type_map: maps part number to a human readable name
        :param reduction_map: not used by this strategy.
        :return: void
        """
        data_ref["itemCount"] = data_ref.apply(lambda row: self.apply_unit_reduction(row["PartNumber"],
                                                                                     row["itemCount"],
                                                                                     reduction_map),
                                               axis=1)
