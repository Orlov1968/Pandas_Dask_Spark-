import dask.dataframe as dd
import time


# Создаю объект Data Frame, удаляю дубликаты из таблицы и пустые ячейки заменяю на 0
def create_df():
    df = dd.read_csv('5000 Sales Records.csv', encoding='UTF-8')
    df = df.drop_duplicates()
    df = df.fillna(0)
    return df


# Работа с объектом Data Frame:

# 1. Фильтрация данных по столбцу
def filter_column():
    filter_df = create_df()
    filter_df = filter_df[filter_df['Region'] == 'Europe']
    return filter_df


# 2. Выполнение расчётов
def average_value():
    df_value = create_df()
    value = df_value['Unit Cost'].mean()  # Вычисление среднего значения по столбцу,
    # но вычисления не выполняются. Создаётся граф задач, который является последовательностью
    # вычислительных шагов.
    result = value.compute()  # Выполняется вычисление. Dask реализует их параллельно или
    # распределёно, в зависимости от конфигурации
    return result


# Группировка данных по одному столбцу('Region') и вычисление суммы для сформированных
# групп по другому столбцу ('Total Profit')
def sum_for_group():
    df = create_df()
    sum_group = df.groupby('Region')['Total Profit'].sum()
    return sum_group


# Сортировка по столбцу в порядке убывания
def sort_column():
    df = create_df()
    df_sort = df.sort_values(by='Total Profit', ascending=False)
    return df_sort


# Расчёт времени вычисления
def sum_column():
    start = time.time()
    df = create_df()
    value = df['Total Cost'].sum()
    result = value.compute()
    finish = time.time()
    time_of_work = (finish - start) * 1000
    print(f'Сумма по столбцу Total Cost :{result}')
    print(f'Время выполнения операции: {time_of_work}')  # 117.8 ms /181.91 /5305.4 / 9980.9 / 33176.8


# для df = dd.read_csv('*.csv', encoding='UTF-8') - 47637.04 ms

sum_column()
