import datetime


def make_request(
        stations: list = None,
        start_date: datetime.date = datetime.date(day=1, month=1, year=1990),
        end_date: datetime.date = datetime.date.today(),
):
    import requests

    if not stations:
        stations = get_all_stations()
    stations_request_str = "".join([f"&station={station}" for station in stations])

    start_day = start_date.day
    start_month = start_date.month
    start_year = start_date.year

    end_day = end_date.day
    end_month = end_date.month
    end_year = end_date.year

    print(stations_request_str)
    url = fr'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?network=BR__ASOS{stations_request_str}&data=all&' \
          f'year1={start_year}&month1={start_month}&day1={start_day}&year2={end_year}&month2={end_month}&day2={end_day}' \
          f'&tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3&report_type=4'

    print(f'URL utilizado: {url}')
    print('Fazendo a requisição...')
    response = requests.get(url)

    print(response.status_code)
    conteudo = response.content.decode('utf-8')
    print(conteudo)
    with open('dados.csv', 'w') as file:
        file.write(conteudo)
        print('Conteudo escrito no arquivo dados.csv')


def get_all_stations() -> list:
    import json
    raw_json = json.loads(get_html_from_url(
        url='https://mesonet.agron.iastate.edu/geojson/network/BR__ASOS.geojson?only_online=0'
    ))
    # print(raw_json)
    icao_codes = []
    for feature in raw_json['features']:
        # print(f"Feature: {feature['id']}")
        icao_codes.append(feature['id'])
    return icao_codes


def get_html_from_url(url: str, verbose: bool = True) -> str:
    import requests
    response = requests.get(url)
    if verbose:
        print(f"URL utilizado: {url}")
        print(f"Status da requisição pra url: {response.status_code}")
    html_content = response.text
    return html_content


if __name__ == '__main__':
    from datetime import date

    start_date = date(day=1, month=1, year=2025)
    end_date = date(day=31, month=1, year=2025)
    make_request(
        stations=['SBAM'],
        start_date=start_date,
        end_date=end_date
    )
