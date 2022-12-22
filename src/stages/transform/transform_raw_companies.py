class TransformRawCompanies:

    def transform(self, companies):
        # tipagem das conlunas
        companies.printSchema()

        # define nome para as colunas
        companies_col_names = [
            'cnpj_basico',
            'razao_social_nome_empresarial',
            'natureza_juridica',
            'qualificacao_do_responsavel',
            'capital_social_da_empresa',
            'porte_da_empresa',
            'ente_federativo_responsavel'
        ]

        # renomea colunas
        for index, col_name in enumerate(companies_col_names):
            companies = companies.withColumnRenamed(f"_c{index}", col_name)

        return companies
