```python
import XXX

# Запрос на генерацию файлов
def create_files_request(business_dt):

# Копирование файлов с s3 на локальный диск ЕТЛ сервера
def get_files_from_s3(business_dt,s3_conn_name):

# Загрузка данных из файлов в БД
def load_file_to_pg(filename,pg_table,conn_args):

# Выполенение запросов в БД
def pg_execute_query(query,conn_args):
# Выполенение запросов в БД по обновлению таблиц с размерностями

dim_upd_sql_query = '''
/*
    update d_customer
    update d_city
    update d_item
    update d_category
    update d_calendar
*/
'''
# Выполенение запросов в БД по обновлению таблиц с фактами
facts_upd_sql_query = '''
/*
    update f_activity
    update f_daily_sales
    update f_research
*/
'''

dag = DAG(

# Шаг 1 - выполнение запроса на создание файлов
create_files_request = PythonOperator(task_id='create_files_request',
                                    python_callable=create_files_request,
                                    op_kwargs={'business_dt': business_dt},
                                    dag=dag)

# Шаг 2 - Копирование файлов с s3 на ЕТЛ сервер
get_files = PythonOperator(task_id='get_files_task',
                                                python_callable=get_files_from_s3,
                        op_kwargs={'business_dt': business_dt,
                                                                    's3_conn_name': s3_conn_name},
                        dag=dag)

# Шаг 3.1 - Запись данных из файла load_customer_research в базу данных
load_customer_research = PythonOperator(task_id='load_customer_research',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': business_dt.replace('-','') + '_customer_research.csv',
                                                'pg_table': 'staging.customer_research',
                                                'conn_args': pg_conn},
                                    dag=dag)
# Шаг 3.2 - Запись данных из файла load_user_order_log в базу данных
load_user_order_log = PythonOperator(task_id='load_user_order_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': business_dt.replace('-','') + '_user_order_log.csv',
                                                'pg_table': 'staging.user_order_log',
                                                'conn_args': pg_conn},
                                    dag=dag)
# Шаг 3.3 - Запись данных из файла load_user_activity_log в базу данных
load_user_activity_log = PythonOperator(task_id='load_user_activity_log',
                                    python_callable=load_file_to_pg,
                                    op_kwargs={'filename': business_dt.replace('-','') + '_user_activity_log.csv',
                                                'pg_table': 'staging.user_activity_log',
                                                'conn_args': pg_conn},
                                    dag=dag)
# Шаг 4 - Обновление таблиц с размерностями
update_dimensions = PythonOperator(task_id='update_dimensions',
                                    python_callable=pg_execute_query,
                                    op_kwargs={'query': dim_upd_sql_query,
                                                'conn_args': pg_conn},
                                    dag=dag)
# Шаг 5 - Обновление таблиц с фактами
update_facts = PythonOperator(task_id='update_facts',
                                    python_callable=pg_execute_query,
                                    op_kwargs={'query': facts_upd_sql_query,
                                                'conn_args': pg_conn},
                                    dag=dag)
# Последовательность шагов
create_files_request >> get_files >> [load_customer_research, load_user_order_log, load_user_activity_log] >> update_dimensions >> update_facts
```
