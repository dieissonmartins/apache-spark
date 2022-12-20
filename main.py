from src.drivers.csv_collector import CsvCollector
from src.stages.extract.extract_companies import ExtractCompanies

html_collector = CsvCollector()

# carrega dados do csv
extract_companies = ExtractCompanies(html_collector)
extract_companies_data = extract_companies.extract()
