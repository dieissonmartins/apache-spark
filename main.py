from src.drivers.csv_collector import CsvCollector
from src.stages.extract.extract_companies import ExtractCompanies
from src.stages.transform.transform_raw_companies import TransformRawCompanies

html_collector = CsvCollector()

# carrega dados do csv
extract_companies = ExtractCompanies(html_collector)
extract_companies_data = extract_companies.extract()

# transforma dados
transform_raw_companies = TransformRawCompanies()
transform_raw_companies_data = transform_raw_companies.transform(extract_companies_data)

transform_raw_companies_data.printSchema()