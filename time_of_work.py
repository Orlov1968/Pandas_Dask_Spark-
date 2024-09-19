import dask.dataframe as dd
import pandas as pd
import time
import matplotlib.pyplot as plt

list_files = ['100 Sales Records.csv', '5000 Sales Records.csv', '500000 Sales Records.csv',
              '1500000 Sales Records.csv', '5m Sales Records.csv', ]
list_size_files = [5.0, 181.3, 17680.7, 53284.5, 197163.5, ]


# Расчёт времени вычисления для dask
def time_calculation_dask():
    list_time_dask = []
    for i in list_files:
        start = time.time()
        df = dd.read_csv(i, encoding='UTF-8')
        df = df.drop_duplicates()
        df = df.fillna(0)
        value = df['Total Cost'].sum()
        result = value.compute()
        finish = time.time()
        time_work = (finish - start) * 1000
        list_time_dask.append(time_work)
    return list_time_dask


# Расчёт времени вычисления для pandas
def time_calculation_pandas():
    list_time_pandas = []
    for i in list_files:
        start = time.time()
        table = pd.read_csv(i, encoding='UTF-8')
        table = table.drop_duplicates()
        table = table.fillna(0)
        value = table['Total Cost'].sum()
        finish = time.time()
        time_work = (finish - start) * 1000
        list_time_pandas.append(time_work)
    return list_time_pandas


# Построение графика зависимости скорости вычисления от объёма файла
x = list_size_files
y1 = time_calculation_pandas()
y2 = time_calculation_dask()
plt.title('Скорость вычисления от объёма данных')
plt.xlabel('объём файла в kB')
plt.ylabel('время вычисления в ms')
plt.grid()
plt.plot(x, y1, x, y2)
plt.show()
