# Проверка качества данных в ETL процессе.

### Задача
1. Проанализировать действующий ETL процесс и решить на каком этапе лучше добавить проверки; <P><P>
2. Написать и внедрить код проверки;
3. Сделать таблицу логов.


#### Схема процесса.

ссылка на dag
ссылка на картинку схемы

#### Первый шаг — create_files_request. Подготовка файлов с данными. На этом этапе отправляется запрос на создание файлов.

#### Второй шаг — get_files. Во время его выполнения данные скачиваются на локальные диски ЕTL-сервера.

#### Третий шаг — load_customer_research, load_user_order_log, load_user_activity_log. Теперь каждый файл загружается в stage-область. Эти три задачи выполняются параллельно.

#### Четвёртый шаг — update_dimensions. Обновление всех таблиц с размерностями в витрине данных.

#### Пятый шаг — update_facts. Обновление всех таблиц фактов в витрине.

Добавляем проверки:

1. Сначала проверяем соответствие наименованию файлов (после шага get_files);
2. Далее необходимо выявить null значения и количество строк в исходной таблице (с помощью sql после загрузки load_order_log и load_user_activity_log).

ссылка на схему

#### Проверить наличие файла проще всего с помощью оператора FileSencor

```python
from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow"
}

task_id="waiting_for_file_имя_файла"

with DAG(
    dag_id="Sprin4_Task1",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
    ) as dag:

    # Так как в базу попадают файлы с датой, то тоже преобразуем
    with TaskGroup(group_id='group1') as fg1:
        date = str(datetime.now().date())
        f1 = FileSensor(task_id='waiting_for_file_customer_research', fs_conn_id ='fs_local', filepath='data/' + date + '_' + '_customer_research.csv', poke_interval = 15, timeout=15 )
        f3 = FileSensor(task_id='waiting_for_file_user_activity_log', fs_conn_id ='fs_local', filepath='data/' + date + '_' + '_user_activity_log.csv', poke_interval = 15, timeout=15 )
        f2 = FileSensor(task_id='waiting_for_file_user_order_log', fs_conn_id ='fs_local', filepath='data/' + date + '_' + '_user_order_log.csv', poke_interval = 15, timeout=15 )

f1 >> f2 >> f3
```

#### Выявить null значения и посчитать количество уникальных строк, можно с помощью SQLCheckOperator или SQLValueCheckOperator.

```python
from airflow import DAG
from datetime import datetime
from airflow.operators.sql import SQLCheckOperator
from airflow.operators.sql import SQLValueCheckOperator

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"
}

with DAG(
    dag_id="Sprin4_Task1",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
    ) as dag:

    # Проверяем на null
    sql_check  = SQLCheckOperator(
        task_id="user_order_log_isNull",
        sql="user_order_log_isNull_check.sql"
        )
    sql_check2  = SQLCheckOperator(
        task_id="user_activity_log_isNull",
        sql="user_activity_log_isNull_check.sql"
        )

    # Проверяем чтобы записей было больше 3
    sql_check3 = SQLValueCheckOperator(
        task_id="check_row_count_user_order_log",
        sql="Select count(distinct(customer_id)) from user_order_log",
        pass_value=3
        )
    sql_check4 = SQLValueCheckOperator(
        task_id="check_row_count_user_activity_log",
        sql="Select count(distinct(customer_id)) from user_activity_log",
        pass_value=3
        )

    sql_check >> sql_check2 >> sql_check3 >> sql_check4
```

#### Создадим таблицу для записи результатов проверки (лог).
