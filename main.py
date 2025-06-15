from src.allianz_data_extractor import get_allianz_data
from src.tradingeconomics_data_extractor import get_tradingeconomics_data
from src.oecd_data_extractor import get_oecd_data
from src.worldbank_data_extractor import get_worldbank_data
from src.countryeconomy_data_extractor import get_country_economy_data
from src.country_names_extractor import get_countries


def main():
    
    get_worldbank_data()
    get_country_economy_data()
    get_allianz_data()
    get_tradingeconomics_data()
    get_oecd_data()
    #get_countries()
    print("Completed")


if __name__ == "__main__":
    main()