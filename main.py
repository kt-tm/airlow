import os
import datetime as dt
import pandas as pd


# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'schedule_interval': '@hourly',
    'start_date': dt.datetime(2021, 3, 29),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

print(get_path('titanic.csv'))

def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')

download_titanic_dataset()
def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))
pivot_dataset()
def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.groupby(['Pclass'])['Fare'].mean()
    df.to_csv(get_path('titanic_mean_fares.csv'))
mean_fare_per_class()