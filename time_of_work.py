import dask.dataframe as dd
import pandas as pd
import time
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

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
fig, ax = plt.subplots(figsize=(8, 6))
ax.set_title("Графики зависимостей: y1 - pandas, y2 - dask", fontsize=16)
ax.set_xlabel("Объём файла, кВ", fontsize=14)
ax.set_ylabel("Время вычисления, ms", fontsize=14)
ax.grid(which="major", linewidth=1.2)
ax.grid(which="minor", linestyle="--", color="gray", linewidth=0.5)
ax.plot(x, y1, c="red", label="pandas")
ax.plot(x, y2, label="dask")
ax.legend()
ax.xaxis.set_minor_locator(AutoMinorLocator())
ax.yaxis.set_minor_locator(AutoMinorLocator())
ax.tick_params(which='major', length=10, width=2)
ax.tick_params(which='minor', length=5, width=1)
plt.show()

