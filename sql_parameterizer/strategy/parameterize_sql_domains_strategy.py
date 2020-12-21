from sql_parameterizer.strategy.abstract_parameterize_sql_strategy import ParameterizeSqlStrategy
from string import Template


class ParameterizeSqlDomainsStrategy:

    @staticmethod
    def generate_query_string(account_guid, domains):
        template = "($account_guid,$domains),"

        result = Template(template).substitute(
            account_guid=account_guid,
            domains=domains
        )

        return result

    def parameterize_df_to_sql(self, data_ref):
        print("Recieved DF")
        print(data_ref.head())
        print(data_ref.columns)

        data_ref["domains_sql_insert"] = data_ref.apply(lambda row: self.generate_query_string(
            row["accountGuid"], row["domains"]
        ), axis=1)
