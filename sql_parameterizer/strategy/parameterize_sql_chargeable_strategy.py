from sql_parameterizer.strategy.abstract_parameterize_sql_strategy import ParameterizeSqlStrategy
from string import Template
from utils.sql_string_sanitizer import sanitize_string



class ParameterizeSqlChargeableStrategy(ParameterizeSqlStrategy):

    @staticmethod
    def generate_query_string(partner_id, product, partner_purchased_plan_id, plan, usage):
        template = "($partner_id, $product, $partner_purchased_plan_id,$plan, $usage),"

        result = Template(template).substitute(
            partner_id=partner_id,
            product=product,
            partner_purchased_plan_id=partner_purchased_plan_id,
            plan=plan,
            usage=usage
        )

        return result

    def parameterize_df_to_sql(self, data_ref):
        data_ref["chargeable_sql_insert"] = data_ref.apply(lambda row: self.generate_query_string(
            int(sanitize_string(str(row["PartnerID"]))),
            sanitize_string(row["PartNumber_mapped"]),
            sanitize_string(row["accountGuid"]),
            sanitize_string(row["plan"]),
            int(sanitize_string(str(row["itemCount"]))),
        ), axis=1)
