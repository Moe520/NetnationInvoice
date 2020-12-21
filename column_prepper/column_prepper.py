class ColumnPrepper:
    def __init__(self, prep_column_strategy):
        self.prepping_strategy = prep_column_strategy

    def prep_column(self, data_ref,type_map,reduction_map):
        self.prepping_strategy.prep_column(self.prepping_strategy, data_ref, type_map,reduction_map)
