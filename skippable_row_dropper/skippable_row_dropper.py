class SkippableRowDropper:
    def __init__(self, drop_rows_strategy):
        self.dropping_strategy = drop_rows_strategy

    def drop_rows(self, data_ref):
        self.dropping_strategy(data_ref)
