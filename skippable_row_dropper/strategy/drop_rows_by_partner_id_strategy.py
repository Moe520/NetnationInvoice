from skippable_row_dropper.strategy import abstract_drop_rows_strategy


class DropRowsByPartnerIdStrategy(abstract_drop_rows_strategy):
    BAD_PARTNER_ID_LIST = {26392}

    def drop_bad_rows(self, data_ref, error_logger):
        data_ref.drop(data_ref[data_ref.PartnerID in self.BAD_PARTNER_ID_LIST], inplace=True)
