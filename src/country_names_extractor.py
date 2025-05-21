import csv
import requests

def get_country_names(output='countries.csv'):
    url = 'https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv'

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error downloading the CSV: {e}")
        return

    lines = response.text.strip().splitlines()
    reader = csv.DictReader(lines)

    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Country', 'Alpha-3'])

        for row in reader:
            writer.writerow([row['name'], row['alpha-3']])

    print(f"Countries saved to {output}")

get_country_names()

