import time
import pandas as pd


# Создаю объект Data Frame, удаляю дубликаты из таблицы и пустые ячейки заменяю на 0
def create_pd():
    table = pd.read_csv('5000 Sales Records.csv', encoding='UTF-8')
    table = table.drop_duplicates()
    table = table.fillna(0)
    return table


# 1. Фильтрация данных по столбцу
def filter_column():
    filter_pd = create_pd()
    filter_pd = filter_pd[filter_pd['Region'] == 'Europe']
    return filter_pd


# 2. Выполнение расчётов
def average_value():
    pdd = create_pd()
    mean_column = pdd['Unit Cost'].mean()  # Вычисление выполняется сразу
    return mean_column


# Группировка по столбцу и получение суммы всех столбцов по сформированным группам
def sum_of_column():
    df = create_pd()
    sum_region = df['Region'].sum()
    return sum_region


# Сортировка по столбцу в порядке убывания
def sort_column():
    df = create_pd()
    df_sort = df.sort_values(by='Total Profit', ascending=False)
    return df_sort


# Расчёт времени вычисления
def sum_column():
    start = time.time()
    df = create_pd()
    value = df['Total Cost'].sum()
    finish = time.time()
    time_of_work = (finish - start) * 1000
    print(f'Сумма по столбцу Total Cost :{value}')
    print(f'Время выполнения операции: {time_of_work}')  # 12.9ms/ 53.62/ 3430.2/ 10288.6/ 36584.2


sum_column()
