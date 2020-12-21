class ColumnPrepper:
    def __init__(self, prep_column_strategy):
        self.prepping_strategy = prep_column_strategy

    def prep_column(self, data_ref,type_map,reduction_map):
        """
        Will transform a column based on a given strategy
        :param data_ref: Reference to a pandas dataframe
        :param type_map: used when mapping part number to item number
        :param reduction_map: used to reduce quantities for certain items
        :return:
        """
        self.prepping_strategy.prep_column(self.prepping_strategy, data_ref, type_map,reduction_map)
