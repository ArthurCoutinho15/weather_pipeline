import pandas as pd
from os.path import join
from datetime import datetime, timedelta


def main():

    data_inicio = datetime.today()
    data_fim = data_inicio + timedelta(days=7)

    data_inicio = data_inicio.strftime('%Y-%m-%d')
    data_fim = data_fim.strftime('%Y-%m-%d')

    city = 'Boston'
    key = 'SNPMQPA3UJBVZZDFEYKZ9EZG8'

    URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
               f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')

    dados = pd.read_csv(URL)
    print(dados.head())
    path = '/home/arthur/weather_pipeline/data'
    dados.to_csv(
        f'{path}/{data_inicio}.csv', index=False)

    dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(
        f'{path}/temperaturas.csv')
    dados[['datetime', 'description', 'icon']].to_csv(f'{path}/condicoes.csv')


if __name__ == '__main__':
    main()
