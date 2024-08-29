# Проверка качества данных в ETL процессе.

### Задача
1. Проанализировать действующий ETL процесс и решить на каком этапе лучше добавить проверки; <P><P>
2. Написать и внедрить код проверки;
3. Сделать таблицу логов.


#### Схема процесса.

[Шаблон DAG](</Проверка качества данных в ETL процессе\dag\dag_forma.md>)

![4. Проверка качества данных/Схема_DAG.png](https://github.com/EvgeniyLezh/data-engineer-yandex-practicum/blob/32d684c597f69072f5546c8ecd329f70eed60f3e/4.%20%D0%9F%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B0%20%D0%BA%D0%B0%D1%87%D0%B5%D1%81%D1%82%D0%B2%D0%B0%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85/%D0%A1%D1%85%D0%B5%D0%BC%D0%B0_DAG.png)

#### Первый шаг — create_files_request. Подготовка файлов с данными. На этом этапе отправляется запрос на создание файлов.

#### Второй шаг — get_files. Во время его выполнения данные скачиваются на локальные диски ЕTL-сервера.

#### Третий шаг — load_customer_research, load_user_order_log, load_user_activity_log. Теперь каждый файл загружается в stage-область. Эти три задачи выполняются параллельно.

#### Четвёртый шаг — update_dimensions. Обновление всех таблиц с размерностями в витрине данных.

#### Пятый шаг — update_facts. Обновление всех таблиц фактов в витрине.

Добавляем проверки:

1. Сначала проверяем соответствие наименованию файлов (после шага get_files);
2. Далее необходимо выявить null значения и количество строк в исходной таблице (с помощью sql после загрузки load_order_log и load_user_activity_log).

![4. Проверка качества данных/Проверки_Схема_DAG.png](https://github.com/EvgeniyLezh/data-engineer-yandex-practicum/blob/32d684c597f69072f5546c8ecd329f70eed60f3e/4.%20%D0%9F%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B0%20%D0%BA%D0%B0%D1%87%D0%B5%D1%81%D1%82%D0%B2%D0%B0%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85/%D0%9F%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B8_%D0%A1%D1%85%D0%B5%D0%BC%D0%B0_DAG.png)


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

dq_checks_results содержит имя таблицы, наименование проверки, дата выполнения, результат выполнения.

```sql
drop table if exists dq_checks_results;
create table dq_checks_results (
 Table_name varchar(255),
 DQ_check_name varchar(255),
 Datetime timestamp,
 DQ_check_result numeric(8,2));
```

Записывать в таблицу будем с помощью Airflow Callbacks:

```python
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
)


def check_success_insert_user_order_log (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull' ,current_date, 0 )
          """)


def check_failure_insert_user_order_log (context):
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            values ('user_order_log', 'user_order_log_isNull' ,current_date, 1 )
          """)


default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "conn_id": "postgres_default"}

with DAG(dag_id="Sprin4_Task61", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    begin = DummyOperator(task_id="begin")
    sql_check = SQLCheckOperator(task_id="user_order_log_isNull", sql="user_order_log_isNull_check.sql" , on_success_callback = check_success_insert_user_order_log, on_failure_callback =  check_failure_insert_user_order_log )

    begin >> [sql_check]>> end
```

Остается только следить за таблицей логов, выявлять и исправлять нарушения.
