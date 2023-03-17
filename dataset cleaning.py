import os
import urllib.request
import numpy as np
import pandas as pd
import seaborn as sns
import zipfile
import matplotlib
import matplotlib.pyplot as plt
from openpyxl.reader.excel import load_workbook
import warnings


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
plt.style.use('ggplot')
matplotlib.rcParams['figure.figsize'] = (16, 9)
pd.options.mode.chained_assignment = None


# Создание новой директории
newpath = r'D:/python/Information_about_job_vacancies_for_people_with_disabilities/source'
if not os.path.exists(newpath):
    os.makedirs(newpath)


# Скачивание информации с открытого портала
print('Beginning file download with https://data.mos.ru/opendata...')
url = 'https://op.mos.ru/EHDWSREST/catalog/export/get?id=1480497'
urllib.request.urlretrieve(url, 'D:/python/'
    'Information_about_job_vacancies_for_people_with_disabilities'
    '/source/data-440-2022-12-23.zip')


# Разархивирование
archive = 'D:/python/' \
          'Information_about_job_vacancies_for_people_with_disabilities' \
          '/source/data-440-2022-12-23.zip'
with zipfile.ZipFile(archive, 'r') as zip_file:
    zip_file.extract('data-440-2022-12-23.xlsx', 'D:/python/'
    'Information_about_job_vacancies_for_people_with_disabilities'
    '/source')

with warnings.catch_warnings(record=True):
    warnings.simplefilter("always")
    myexcelfile = pd.read_excel('D:/python/'
        'Information_about_job_vacancies_for_people_with_disabilities'
        '/source/data-440-2022-12-23.xlsx', engine="openpyxl")


# Удаление ненужной информации
wb2 = load_workbook('D:/python/'
    'Information_about_job_vacancies_for_people_with_disabilities'
    '/source/data-440-2022-12-23.xlsx')
ws2 = wb2['0']

ws2.delete_rows(2, 1)

# Сохранение изменений
wb2.save('D:/python/'
    'Information_about_job_vacancies_for_people_with_disabilities'
    '/source/data-440-2022-12-23.xlsx')

# Начало обработки датасета
df = pd.read_excel("D:/python/"
    "Information_about_job_vacancies_for_people_with_disabilities"
    "/source/data-440-2022-12-23.xlsx")


print("\nРазмер данных: ", df.shape, '\n')
print("Типы данных:\n", df.dtypes)
print(df.info())

# отбор числовых колонок

df_numeric = df.select_dtypes(include=[np.number])
numeric_cols = df_numeric.columns.values
print("Числовые колонки: ", numeric_cols)


# отбор нечисловых колонок

df_non_numeric = df.select_dtypes(exclude=[np.number])
non_numeric_cols = df_non_numeric.columns.values
print("\nНечисловые колонки: ", non_numeric_cols)

# Тепловая карта пропущенных значений

cols = df.columns
colours = ['#000099', '#ffff00']
sns.heatmap(df[cols].isnull(), cmap=sns.color_palette(colours))
plt.show()

# Процентный список пропущенных данных

for col in df.columns:
    pct_missing = np.mean(df[col].isnull())
    print('{} - {}%'.format(col, round(pct_missing * 100)))


# Гистограмма пропущенных данных по оси X количество пропущенных значений. По оси У количество записей.
# Создаем индикатор для признаков с пропущенными данными

for col in df.columns:
    missing = df[col].isnull()
    num_missing = np.sum(missing)
    if num_missing > 0:
        # print('created missing indicator for: {}'.format(col))
        df['{}_ismissing'.format(col)] = missing

# На основе индикатора строим гистограмму

ismissing_cols = [col for col in df.columns if 'ismissing' in col]
df['num_missing'] = df[ismissing_cols].sum(axis=1)
df['num_missing'].value_counts().reset_index().sort_values(by='index').plot.bar(x='index', y='num_missing')
plt.show()


# Лишь небольшое количество строк содержат более 38 пропусков. Отбросим строки которые содержат 39 пропусков

ind_missing = df[df['num_missing'] > 38].index
df = df.drop(ind_missing, axis=0)

# Отбрасывание признаков. Отбросим все, которые имеют высокий процент недостоящих значений >= 75%

cols_to_drop = [['Skills'], ['InterviewPlaceAdmArea'], ['InterviewPlaceDistrict'], ['InterviewPlaceLocation'],
                ['InterviewPlaceNote'], ['WorkPlaceNote'], ['Prof_en'], ['Number_en'],
                ['Date_en'], ['CountVacancy_en'], ['SpecialWorkPlace_en'], ['Specification_en'], ['MinZarplat_en'],
                ['MaxZarplat_en'], ['WorkFunction_en'], ['WorkRegim_en'], ['WorkOsob_en'], ['Skills_en'],
                ['DopWorkersParameters_en'], ['WorkType_en'], ['Education_en'], ['ProfStage_en'], ['InterviewPlaceAdmArea_en'],
                ['InterviewPlaceDistrict_en'], ['InterviewPlaceLocation_en'], ['InterviewPlaceNote_en'], ['WorkPlaceAdmArea_en'],
                ['WorkPlaceDistrict_en'], ['WorkPlaceLocation_en'], ['WorkPlaceNote_en'], ['FullName_en'],
                ['ContactName_en'], ['Phone_en'], ['Email_en'], ['geodata_center'], ['geoarea']]

for i in cols_to_drop:
    df = df.drop(i, axis=1)

cols = df.columns[:23]
colours = ['#000099', '#ffff00']
sns.heatmap(df[cols].isnull(), cmap=sns.color_palette(colours))
plt.show()

# Для числовых признаков заменим все недостающие значение медианной этого признака
df_numeric = df.select_dtypes(include=[np.number])
numeric_cols = df_numeric.columns.values

for col in numeric_cols:
    missing = df[col].isnull()
    num_missing = np.sum(missing)

    if num_missing > 0:  # only do the imputation for the columns that have missing values.
        print('imputing missing values for: {}'.format(col))
        df['{}_ismissing'.format(col)] = missing
        med = df[col].median()
        df[col] = df[col].fillna(med)


# Тепловая карта пропущенных значений

cols = df.columns[:23]
colours = ['#000099', '#ffff00']
sns.heatmap(df[cols].isnull(), cmap=sns.color_palette(colours))
plt.show()

# Добавим недостающие значения дефолтными

df['ContactName'] = df['ContactName'].fillna('Нет данных по ФИО')
df['Phone'] = df['Phone'].fillna('Нет данных по телефону')
df['Specification'] = df['Specification'].fillna('Нет данных по спецификации')
df['DopWorkersParameters'] = df['DopWorkersParameters'].fillna('Нет данных по дополнительным параметрам')

# Обнаружение и удаление дубликатов
# Проверим на наличие дубликатов global_id, а остальные стоблцы могут быть не уникальными

if df.global_id.nunique(dropna=True) == len(df.index):
    print("Дубликаты не обнаружены")
else:
    df = df.drop_duplicates('global_id')

# Приведем дату к типу datetime

df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)


# Удалим столбцы созданные для гистограммы

df.drop(df.iloc[:, 23:], inplace=True, axis=1)

# Удалим лишние пробелы и точки

df_non_numeric = df.select_dtypes(exclude=[np.number])
for column in df_non_numeric:
    if column not in ['Date', 'SpecialWorkPlace', 'geoData']:
        df[column] = df[column].str.strip()
        df[column] = df[column].str.replace('\.', '', regex=True)

# Тепловая карта пропущенных значений

cols = df.columns
colours = ['#000099', '#ffff00']
sns.heatmap(df[cols].isnull(), cmap=sns.color_palette(colours), vmin=0, vmax=1)
plt.show()

print(df.head())

# Закинем в xlsx файл для дальнейшей работы

df.to_csv("D:/python/"
    "Information_about_job_vacancies_for_people_with_disabilities"
    "/source/output.csv", index=False)