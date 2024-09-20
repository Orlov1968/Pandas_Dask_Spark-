from pyspark.sql import SparkSession

from pyspark.sql.functions import col

# Создаю сессию
spark = SparkSession.builder.appName('diplomWork').getOrCreate()


def create_psd():
    psd = spark.read.csv('100 Sales Records.csv', header=True)
    psd = psd.fillna(value=0)
    return psd


def filter_column():
    filter_df = create_psd()
    filter_df = filter_df.filter(col('Region') == 'Europe')
    return filter_df


def average_value():
    df_value = create_psd()
    value = df_value.agg({'Unit Cost': 'avg'})
    return value


def sum_of_column():
    df = create_psd()
    sum_unit = df['Unit Cost'].sum()
    return sum_unit


def sort_column():
    df = create_psd()
    df_sort = df.sort_values(by='Total Profit', ascending=False)
    return df_sort
