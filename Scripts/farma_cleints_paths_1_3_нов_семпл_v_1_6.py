import numpy as np
import pandas as pd
import datetime as dt
import time
import openpyxl
pd.options.mode.chained_assignment = None #  убираем предупреждение
##  настраиваем ширину вывода в консоли в символах и в столбцах
pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 10)
##

print('start')
print("current time:-", dt.datetime.now())


df = pd.read_csv(
    # Sampled base
    #r'c:\Users\aa_ryabukhin\Documents\С_Рябухин_рабочая\аптека ру динамика клиентов\_dct_dyn_Повторные покупки клиентов АпРу_SAMPLED.csv'
    # Whole base
    r'c:\Users\aa_ryabukhin\Documents\С_Рябухин_рабочая\аптека ру динамика клиентов\_dct_dyn_Повторные покупки клиентов АпРу.csv'
                #,delimiter = ';'
                ,delimiter = '\t'
                ,decimal =','
                #,encoding = 'utf'
                 ,low_memory=False  # ругается на разные типы данных в столбцах 6-7, вроде лоу-мемори флаг этодолжен пофиксить
                ,encoding = 'windows-1251' # по умолчанию utf-8
                ,encoding_errors = 'replace'
                #,nrows =1000 # чтобы не весь файл пока тащить]
                )
# часть дат распознается неверно, поэтому его удалим, тем более есь столбец date_id а также год и месяц
df.drop('канал укрупненно_зс-протек отдельно отдельно', axis= 1 , inplace= True)
df.rename(columns = {'quantity|Сумма':'quantity', 'fact_grs|Сумма':'fact_grs'}, inplace = True )
df.rename(columns = {'канал укрупненно': 'kanal_ukrupnenno'}, inplace = True )
df.rename(columns = {'канал': 'kanal'}, inplace = True )
df.rename(columns = {'тип клиента': 'client_type'}, inplace = True )
df.rename(columns = {'тип площадки': 'store_type'}, inplace = True )
df.rename(columns = {'сайт_stand': 'site_stand'}, inplace = True )

df['date_id'] = df['date_id'].astype('str')
df['phone_clear'] = df['phone_clear'].astype('str')
df['internet_order_id'] = df['internet_order_id'].astype('str')

df.info()
#df.head(3)
print("end of: " + "данные загружены" + "; rows = ",  df.shape[0])
#print(time.time())
print("current time:-", dt.datetime.now())

#  создаем-восстанавливаем верный формат даты из поля date_id
df['date_id'] = df['date_id'].astype('str') #
#df['Date'] = df['Date'].astype("datetime64[ns]")
df['Date'] = df['date_id'].str[6:] +"." + df['date_id'].str[4:6] + "." + df['date_id'].str[0:4]
df['Date']= pd.to_datetime(df['Date'], dayfirst=True)
print("end of: " + "создаем-восстанавливаем верный формат даты из поля date_id")


#  создаем-восстанавливаем верный формат даты из поля date_First_purchese_in_ApRU
df['date_First_purchese_in_ApRU'] = df['date_First_purchese_in_ApRU'].astype('str') #
df['date_1st_purchese_in_ApRU'] = df['date_First_purchese_in_ApRU'].str[6:] +"." + df['date_First_purchese_in_ApRU'].str[4:6] + "." + df['date_First_purchese_in_ApRU'].str[0:4]
df['date_1st_purchese_in_ApRU']= pd.to_datetime(df['date_1st_purchese_in_ApRU'], dayfirst=True)
print("end of: " + "создаем-восстанавливаем верный формат даты из поля date_First_purchese_in_ApRU")



# fact_grs  в ряде случаев в примере = "?", поэтому их удаляем пока
df = df[df.fact_grs !='?']
df['fact_grs'] = df['fact_grs'].replace(',', '.', regex=True)
df['fact_grs'] = df['fact_grs'].astype('float')
#df['fact_grs_n'] = df['fact_grs'].astype('float')

#df['quantity_n '] = df['quantity '].astype('float')
print("end of: " + 'fact_grs  в ряде случаев в примере = "?", поэтому их удаляем пока')
#df.info()
#df.head(2)


#  создаем-восстанавливаем верный формат даты из поля date_id
#df_d = df
#df_d['Date'] = df_d['date_id'].str[6:] +"." + df_d['date_id'].str[4:6] + "." + df_d['date_id'].str[0:4]
#df['Date']= pd.to_datetime(df['Date'])
#df_d.head(2)
print("end of "+ " создаем-восстанавливаем верный формат даты из поля date_id")
"""<font color =  '#f00' >## !!!! Часть дат неверно "переводится", например 2023-01-10 в 01 октября 2023 года!!!**</font>
"""


## !!!! Часть дат неверно "переводится", например 2023-01-10 в 01 октября 2023 года!!!
## -----------------------------------------------------------------------------
# проверка
#df[df['phone_clear'] == 9001123108].sort_values(by=['date_id'])

#df.head()

# присваиваем когорты исходя из даты первой покупки в АпрУ данного "телефона"
df2 = df
df2['cohort'] = df2['date_First_purchese_in_ApRU'].apply(str)
df2['cohort']= df2['cohort'].str[:4] + '-' + df2['cohort'].str[4:6]+ '_cohort'
#df2.head(10)

print("end of; "+ "присваиваем когорты исходя из даты первой покупки в АпрУ данного 'телефона'")

# сортируем по телефону и по дате
df2_paths=df2
df2_paths = df2_paths.drop(['client_type', 'purchases_after_First_purchase_in_ApRu','Day','Week', 'Month', 'Year',  'kanal',
                'site_name', 'brand_name'], axis=1)

df2_paths = df2_paths.sort_values(by=['phone_clear', 'date_id'])
print("end of; "+ "# сортируем по телефону и по дате")


# Добавить "Год-месяц"
#df4['year'] = pd.DatetimeIndex(df4['Date']).year
df2_paths['year_month'] = df2_paths['Date'].dt.to_period("M")
#df4['year_month'] =
df2_paths['year_month'] = df2_paths['year_month'].apply(str)
#df2_paths.head(5)
print("end of; "+ "Добавить 'Год-месяц'")



#df2_paths.info()

#print(df2_paths.head())
# alll iunt and float cinvert to str else - error
df2_paths['fact_grs'] = round(df2_paths['fact_grs'])
df2_paths['fact_grs'] = df2_paths['fact_grs'].astype('str')
df2_paths['fact_grs'] = df2_paths['fact_grs'].astype('str')
df2_paths['Date'] = df2_paths['Date'].astype('str')
df2_paths['fact_grs'] = df2_paths['fact_grs'].astype('str')
df2_paths_raw = df2_paths.copy()
#df2_paths.info()
df2_paths = df2_paths.groupby(['cohort','phone_clear','date_1st_purchese_in_ApRU']).agg('->'.join).reset_index()
print('end of: '+'сформировали когорты')
print("current time:-", dt.datetime.now())



#группировка по телефонам- находим суммарную выручку ПОВТОРНЫХ покупок + # группировка по телефонам и периоды покупки
df3_repited_orders_fact = df2.groupby(['phone_clear']).fact_grs.agg(repited_orders_fact_sum = 'sum').reset_index()
df3_repited_orders_fact.head()

df3 = df2
#df3_orders_count = df3.groupby(by = 'phone_clear').date_id.agg(min_purchase_date = 'min', max_purchase_date = 'max', count_orders = 'count').reset_index()
df3_orders_count = df3.groupby(['phone_clear','date_1st_purchese_in_ApRU']).Date.agg(second_purchase_date = 'min' , max_purchase_date = 'max', count_repited_orders = 'count').reset_index()

print( 'end of: ''группировка по телефонам- находим суммарную выручку ПОВТОРНЫХ покупок ')


#  находим длину периода истории - с первой даты  до текущего дня (?или до последней покупки)
df3_orders_count['duration_first_last'] = (df3_orders_count['max_purchase_date'] - df3_orders_count['date_1st_purchese_in_ApRU']).dt.days
df3_orders_count['duration_first_last_months'] = round((df3_orders_count['max_purchase_date'] - df3_orders_count['date_1st_purchese_in_ApRU']).dt.days / 30 + 1,1)
df3_orders_count['duration_first_today'] = (dt.datetime.now() - df3_orders_count['date_1st_purchese_in_ApRU']).dt.days
df3_orders_count['duration_first_today_months'] = round((dt.datetime.now()- df3_orders_count['date_1st_purchese_in_ApRU']).dt.days / 30 + 1,1)
df3_orders_count['duration_no_action_months'] = round((dt.datetime.now()- df3_orders_count['max_purchase_date']).dt.days / 30 + 1,1)

print('end of: ' + 'находим длину периода истории - с первой даты  до текущего дня (?или до последней покупки)')

# find average orders_per_month
df3_orders_count['avg_repited_orders_count_per_month_last_month'] = round(df3_orders_count['count_repited_orders'] / df3_orders_count['duration_first_last_months'],1)
df3_orders_count['avg_repited_orders_count_per_month_today'] = round(df3_orders_count['count_repited_orders'] / df3_orders_count['duration_first_today_months'],1)
df3_orders_count['client_status'] = df3_orders_count['duration_no_action_months'].apply(lambda x: 'active_client' if x <= 3 else 'sleeping_3+_months_client')

#df3_orders_count.info()
#print(df3_orders_count.head(10))
print('end of; ' + "find average orders_per_month")


# присоединяем   суммы выручки ПОВТОРНЫХ закзов df3_repited_orders_fact
df3_orders_count_sum = df3_orders_count.set_index('phone_clear').join(df3_repited_orders_fact.set_index('phone_clear'), rsuffix='_').reset_index()
df3_orders_count_sum['avg_repited_orders_fact'] = round(df3_orders_count_sum['repited_orders_fact_sum'] / df3_orders_count_sum['count_repited_orders'])
#df3_orders_count_sum.info()
#df3_orders_count_sum.head()
print('end of; '+ 'присоединяем   суммы выручки ПОВТОРНЫХ закзов df3_repited_orders_fact')

# !!! Важно понимать, что тут нет тех клиентов, кто только один раз купил в АпРу и больше нигде не покупал.
# ? Какова их доля?
#df3_orders_count_sum.describe()
#df3.describe()
#df3.info()

# проверяем на примере
#df3_11 = df3
#df3_11 = df3_11[df3_11.phone_clear == 	9001123108]
#df3_11.sort_values(by = ['Date'])
#df3_11.head(3)

# объединяем таблицы df2_paths   и df3_orders_count_sum
df4_orders_paths = df2_paths.set_index('phone_clear').join(df3_orders_count_sum.set_index('phone_clear'), rsuffix='_').reset_index()
#df4_orders_paths.head()
#df4_orders_paths.info()

# !!! перевести в числа разницу между собфтиями и округлить суммы  factgrs
df4_orders_paths['repited_orders_fact_sum']=df4_orders_paths['repited_orders_fact_sum'].round()

print('end of: ' + "объединяем таблицы df2_paths   и df3_orders_count_sum")


# исследуем монотонность "kanal_ukrupnenno"
df_ex = df4_orders_paths.copy()

################ СНЯТЬ ОГРАНИЧЕНИЕ ПРИ ПРОДЕ // обрезаем на время отладки
''''
print('!!!!   ---- СНЯТЬ ОГРАНИЧЕНИЕ ПРИ ПРОДЕ // обрезаем на время отладки')
df_ex = df_ex[df_ex.client_status == "active_client"]
df_ex = df_ex[df_ex.count_repited_orders  < 4]
df_ex = df_ex[df_ex.phone_clear.str.contains('91')]
df_ex = df_ex.head(30)
#############
'''
#df_ex.info()

df_ex=df_ex[['phone_clear', 'count_repited_orders', 'kanal_ukrupnenno']]
#df_ex=df_ex['count_repited_orders']
#df_ex.head(10)
#df_ex.info()
print("start of:" + "исследуем монотонность 'kanal_ukrupnenno'")
print("current time:-", dt.datetime.now())

# создаем функцию и применяем ее  с помощью map(function)
# example
# def salary_stats(value):
# ..if row['used'] == 1.0:
#      return 'Full'
#   elif row['used'] == 0.0:
#       return 'Empty'
# df['salary_stats'] = df['salary'].map(salary_stats)
# https://translated.turbopages.org/proxy_u/en-ru.ru.064ea8fe-64936f20-fc97c4d8-74722d776562/https/www.geeksforgeeks.org/create-a-new-column-in-pandas-dataframe-based-on-the-existing-columns/


############## визуализация пошагово, однако не работает
# from IPython.core.interactiveshell import InteractiveShell
# InteractiveShell.ast_node_interactivity = "all"
##############

df_ex3 = df_ex.copy()

########### def function
def paths_to_tend(path): # path as string
  first_step = path.split('->')[0] #пример - первое значение
  steps_list = path.split('->')
  counter = 0
  #current_step = first_step
  #trend ="?"
  dct_step_types = []
  #dct_step_types.append(first_step)
  for step in steps_list:
    counter += 1
    #current_step = step
    #if counter > 1:
    if step not in dct_step_types:
      dct_step_types.append(step)

  ###
  # надо отсортировать список видов шагов
    dct_step_types.sort()

  # список конвeртируем с строку
  # my_lst_str = ''.join(map(str, my_lst))
  dct_step_types_str = ', '.join(map(str, dct_step_types))

  if counter == 1:
    result =  "1 повторная покупка, в: " + first_step
  else:
    result = "несколько повторных покупок, в: " + dct_step_types_str
    #result = str(counter) + " repited orders in: " + dct_step_types_str


  return result

########### end function
#print ('end of: function def')

# начало обработки таблицы функцией и нахождение монотонности
df_ex3['uniq_steps_sorted'] = df_ex3['kanal_ukrupnenno'].map(paths_to_tend)
#print(df_ex3.head(30))


#  к таблице df4_orders_paths джойним таблицу монотонности
df4_orders_paths = df4_orders_paths.set_index('phone_clear').join(df_ex3[['phone_clear', 'uniq_steps_sorted']].set_index('phone_clear'), rsuffix='_').reset_index()

#print('после джойна проверка')
#df4_orders_paths.info()


df_ex4 = df_ex3.copy()
# ghbvth elfkbnm df3_repited_orders_fact = df2.groupby(['phone_clear']).fact_grs.agg(repited_orders_fact_sum = 'sum').reset_index()
df_ex4 =  df_ex4.groupby(['uniq_steps_sorted']).phone_clear.agg(iniq_phone_clear_count = 'count').reset_index()
#df_ex4.info()
#df_ex4.head()
#df_ex4.sort_values(by = ['uniq_steps_sorted'])
df_ex4.sort_values(by = ['iniq_phone_clear_count'], ascending=False)

#df_ex4.info()
#print(df_ex4.head(10))

print('end of: ' + '# начало обработки таблицы функцией и нахождение монотонности')
print("current time:-", dt.datetime.now())


####### доп анализ по лояльности

''' # нужно ли нам это - поиск первых закзов в наши РУ - отдельно таблицей?
print('####### доп анализ по лояльности')
# найти дату первой покупки внашиРу или НашОфлайн
df_ex5_only_our = df2_paths_raw.copy()
# выбираем только нужные столбцы
#df_ex5 = df_ex5[['phone_clear','date_First_purchese_in_ApRU', 'date_id', 'kanal_ukrupnenno']]
df_ex5_only_our = df_ex5_only_our[['phone_clear', 'date_id', 'kanal_ukrupnenno']]
df_ex5_only_our.sort_values(by=['phone_clear', 'date_id'], ascending=[False, True])

#print(df_ex5['kanal_ukrupnenno'].unique()) # проверка каналов

df_ex5_only_our = df_ex5_only_our[(df_ex5_only_our['kanal_ukrupnenno'] == 'НашОфлайн') | \
                                  (df_ex5_only_our['kanal_ukrupnenno'] == 'НашиРу')]
#df_ex5_only_our.info()
#print(df_ex5_only_our.head(30))
#print(df_ex5_only_our.tail(50))

# date_id переводим в формат даты
df_ex5_only_our['date_id'] = df_ex5_only_our['date_id'].astype('str') #
df_ex5_only_our['date_id_norm'] = df_ex5_only_our['date_id'].str[6:] +"." + df_ex5_only_our['date_id'].str[4:6] + "." + df_ex5_only_our['date_id'].str[0:4]
df_ex5_only_our['date_id_norm']= pd.to_datetime(df_ex5_only_our['date_id_norm'], dayfirst=True)

# нахзодим мин даты date_id для кадого телефона (временная таблица)
df_ex5_only_our_mindate = df_ex5_only_our.groupby(['phone_clear']).date_id_norm.agg(min_date_our =  np.min).reset_index()
#df_ex5_only_our_mindate = df_ex5_only_our.groupby(['phone_clear']).agg(min_date_our = ('date_id_norm', np.min), \
 #           count_our_orders = ('phone_clear', 'count')).reset_index()
#print(df_ex5_only_our_mindate.head())

# добавляем минимальные даты в общую таблицу ##### ???? А азчем?
df_ex5_only_our = df_ex5_only_our.set_index('phone_clear').join(df_ex5_only_our_mindate.set_index('phone_clear'), rsuffix='_').reset_index()
#print(df_ex5_only_our.head(15))

#df_ex5_only_our['date_First_purchese_in_OUR'] =
df_ex5_only_our.info()
'''



df_ex5 = df4_orders_paths.copy()
# ставим только год-месяц даты первой покупки
df_ex5['year_month_first_purshase'] = df_ex5['date_First_purchese_in_ApRU'].str[:6]
#


def our_paths_mono(vec): # path as string

  kanal_ukrupnenno = vec[0]
  date_id = vec[1]
  #first_step = kanal_ukrupnenno.split('->')[0] #пример - первое значение
  steps_list = kanal_ukrupnenno.split('->')
  #zero_step = 'Apteka.ru'
  #zero_step_date_id =
  #first_step_date_id = date_id.split('->')[0]  # пример - первое значение
  steps_list_dates = date_id.split('->')

  counter = 0

  dct_step_types = ['НашиРу','НашОфлайн']
  dct_step_dates = []
  #dct_step_types.append(first_step)
  for step in steps_list:

    #current_step = step
    #if counter > 1:
    if step in dct_step_types:
        #first_step_date_id = date_id.split('->')[0]
        #new_step = steps_list_dates[counter] # вариант с полным date_id
        new_step = steps_list_dates[counter][:6]  # вариант с обрезанным  date_id. берем только год месяц

        if counter == 0:
            dct_step_dates.append(new_step)
        #if (counter > 0 ) and (previous_step_date != "_") and (new_step != previous_step_date):
        if (counter > 0) and (new_step != previous_step_date):
            q = 1
            dct_step_dates.append(new_step)
            # тут код для нахождения длительности




        previous_step_date = new_step

      #dct_step_types.append(step)
    else:

        if counter == 0:
            dct_step_dates.append("_")
        else:
            if previous_step_date != "_":
                dct_step_dates.append("_")

        previous_step_date = "_"

    counter += 1

  # список конвeртируем с строку
  dct_step_dates_str = ', '.join(map(str, dct_step_dates))
  result = dct_step_dates_str

  return result

df_ex5['our_dates'] = df_ex5[['kanal_ukrupnenno','date_id']].apply(our_paths_mono, axis=1)


#### в новом столбце будем анализировать длительность лояльности к нам
def our_paths_mono_lenght(our_dates): # path as string

    #first_step = kanal_ukrupnenno.split('->')[0] #пример - первое значение
  months_list = our_dates.split(', ')
  counter = 0

  dct_lengths = []
  dct_lengths2 = []
  mono_steps_count = 0
  first_month = None
  delta_months_max = 0
    #dct_step_types.append(first_step)
  for month in months_list:

    if (month == '_') and (counter == 0):
        #dct_lengths = []
        mono_steps_count = 0
        dct_lengths.append('_')
        previous_month = '_'
        first_month = None
    elif (month == '_') and (counter != 0):
        mono_steps_count = 0
        dct_lengths.append('_')
        previous_month = '_'
        first_month = None
    elif (month != '_'):  #and (previous_month != '_'):
        if first_month == None:
            first_month = month
        #mono_steps_count += 1
        #dct_lengths.append(mono_steps_count)
        previous_month = month
        if (previous_month != '_') and (first_month == month):
            delta_months = 0
            dct_lengths.append(delta_months)
        if (previous_month != '_') and (first_month != month) :
            # вычисляем  разницу между  первым и текущим шагом в непрерывной последовательности
            delta_months = (int(month[:4]) - int(first_month[:4]))*12  \
                          + int(month[4:]) - int(first_month[4:])

        ## вычисляем max разницу между  первым и текущим шагом в непрерывной последовательности
        if delta_months > delta_months_max:
            delta_months_max = delta_months
        #

            dct_lengths.append(delta_months)

    counter += 1

  # список конвeртируем с строку
  dct_months_str = ', '.join(map(str, dct_lengths)) # если пишем последовательность длительностей
  dct_months_str = dct_months_str + "| max = " + str(delta_months_max)  # если пишем максимальный результат к последовательности

  result = dct_months_str

  return result

df_ex5['our_dates_length'] = df_ex5['our_dates'].map(our_paths_mono_lenght)


#df['NewCol'] = df[['TimeCol', 'ResponseCol']].apply(segmentMatch, axis=1)
# пример  df_ex3['uniq_steps_sorted'] = df_ex3['kanal_ukrupnenno'].map(paths_to_tend)



print('работа функции нахождения моно закончена')
df_ex5.info()
#print(df_ex5.head())
#print(df_ex5['year_month_first_pershase'].head(5))
#df_ex5_cut = df_ex5['phone_clear','date_id','kanal_ukrupnenno', 'our_dates']


print(df_ex5[['phone_clear','date_id','kanal_ukrupnenno', 'our_dates','our_dates_length']].head(60))

df4_orders_paths = df4_orders_paths.set_index('phone_clear').join(df_ex5.set_index('phone_clear'), rsuffix='_').reset_index()
print("! макс длительности последовательных месяцев наших покупок - найдены")

#### дропаем лишние колонки, они пока не нужны
df4_orders_paths = df4_orders_paths.drop(columns=[
    'date_id'
    ,'date_First_purchese_in_ApRU'
    ,'date_1st_purchese_in_ApRU_'
    ,'internet_order_id'
    ,'quantity'
    ,'fact_grs'
    ,'kanal_ukrupnenno'
    ,'site_stand'
    ,'store_type'
    ,'Date'
    ,'year_month'
    ,'second_purchase_date'
    ,'date_1st_purchese_in_ApRU'
    ,'duration_first_last'
    ,'duration_first_today'
    ,'avg_repited_orders_count_per_month_today'

])
### добавляем данные об итоговых предпочтениях клиентов (определенным по весам в лоджиноме)
print('start - подгрузка данных о предпочтениях из результатов Лоджинома')
# !!!!!! Проверь, что этот файл обновлен и актуален
###### импорт из экселя стал давать ошибку
#file = r'c:\Users\aa_ryabukhin\Documents\С_Рябухин_рабочая\аптека ру динамика клиентов\_dct-dyn_предпочтения клиентов.xlsx'
#xl = pd.ExcelFile(file)
#print(xl.sheet_names)
#df_preferences= xl.parse('Лист1')
###
df_preferences = pd.read_csv(

    r'c:\Users\aa_ryabukhin\Documents\С_Рябухин_рабочая\аптека ру динамика клиентов\_dct-dyn_предпочтения клиентов.csv'
                #,delimiter = ';'
                ,delimiter = '\t'
                ,decimal =','
                #,encoding = 'utf'
                 ,low_memory=False  # ругается на разные типы данных в столбцах 6-7, вроде лоу-мемори флаг этодолжен пофиксить
                ,encoding = 'windows-1251' # по умолчанию utf-8
                ,encoding_errors = 'replace'
                #,nrows =1000 # чтобы не весь файл пока тащить]
                )



df_preferences.rename(columns = {'канал укрупненно': 'channel_wide_preferred_by_weights'}, inplace = True )
df_preferences.rename(columns = {'brand_name': 'brand_name_preferred_by_weights'}, inplace = True )
df_preferences['phone_clear'] = df_preferences['phone_clear'].astype('str')
df_preferences.info()



 ######### ! проверить на дубли номера телефонов!!!!!!
 ##______________________________________________



print(df_preferences. head())
print('end of - подгрузка данных о предпочтениях из результатов Лоджинома')
print("current time:-", dt.datetime.now())
df4_orders_paths. info()
print(df4_orders_paths. head())
print('start - метчинг большой таблицы с данными о предпочтениях из результатов Лоджинома')

df4_orders_paths = df4_orders_paths.set_index('phone_clear').join(df_preferences.set_index('phone_clear'), rsuffix='_').reset_index()

df4_orders_paths. info()

print('end of - метчинг большой таблицы с данными о предпочтениях из результатов Лоджинома')
print("current time:-", dt.datetime.now())

print(df4_orders_paths. head())



### скрытие номеров телефонов перед записью
df4_orders_paths['phone_clear'] = df4_orders_paths['phone_clear'].str.replace('1','R') \
        .str.replace('2','I') \
        .str.replace('3','G') \
        .str.replace('4','L') \
        .str.replace('5','A') \
# метод ниже не сработал - показывал NAN
#vals_to_replace = {'1':'E', '2':'D', '3':'C'}
#df4_orders_paths['phone_clear']  = df4_orders_paths['phone_clear'].map(vals_to_replace)
###

#print(df4_orders_paths.head())


### запись результатов в таблицы
print('start of: ' + 'запись результатов в таблицы')
#print("current time:-", dt.datetime.now())

work_output_folder = r'c:\Users\aa_ryabukhin\Documents\С_Рябухин_рабочая\аптека ру динамика клиентов\\'

#№№№ агрегированно пути


trend_filename ='_to_BI_output_trends_path.csv'
trend_output_path = work_output_folder + trend_filename
print('writing ' + trend_filename + "; rows = ",  df_ex4.shape[0])



df_ex4.to_csv(
    trend_output_path
    #r'c:\Users\aa_ryabukhin\Documents\С_Рябухин_рабочая\аптека ру динамика клиентов\_to_BI_output_trends_path.csv'
    ,index=False
    , sep=';'
    #,headers=True
              , encoding='windows-1251'  # по умолчанию utf-8

)

# агрегированно тренды-монотонность
paths_filename = '_to_BI_output_orders_paths.csv'
paths_output_path = work_output_folder + paths_filename
print('writing ' + paths_filename + "; rows = ",  df4_orders_paths.shape[0])

df4_orders_paths.to_csv(
    #paths_output_path
    r'c:\Users\aa_ryabukhin\Documents\С_Рябухин_рабочая\аптека ру динамика клиентов\_to_BI_output_orders_paths.csv'
    , index=False
    , sep=';'
    #, headers=True
    , encoding='windows-1251'  # по умолчанию utf-8
    )

print('end of: ' + 'запись результатов в таблицы')
print("current time:-", dt.datetime.now())
#        коммент для проверки гита
### 646dscwerrfwefreef
