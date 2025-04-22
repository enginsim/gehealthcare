from src.allianz_data_extractor import get_allianz_data
from src.tradingeconomics_data_extractor import get_tradingeconomics_data
from src.oecd_data_extractor import get_oecd_data
from src.worldbank_data_extractor import get_wordlbank_data
from src.countryeconomy_data_extractor import get_countryeconomy_data

def main():
    get_oecd_data()
    get_wordlbank_data()
    get_countryeconomy_data()
    get_allianz_data()
    get_tradingeconomics_data()

if __name__ == "__main__":
    main()