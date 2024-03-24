from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
import random
import requests

# Функция для генерации рандомного числа и печати его в консоль
def print_random_number():
    random_number = random.randint(1, 100)
    print(f"Сгенерированное рандомное число: {random_number}")

# Функция для генерации рандомного числа, возвода его в квадрат и вывода в консоль
def generate_and_square_random_number():
    random_number = random.randint(1, 100)
    squared_number = random_number ** 2
    print(f"Исходное число: {random_number}, Результат: {squared_number}")

# Функция для отправки запроса к API погоды и вывода информации о погоде
def get_weather_and_print(location):
    url = f"https://goweather.herokuapp.com/weather/{location}"
    response = requests.get(url)
    if response.status_code == 200:
        weather_data = response.json()
        print(f"Погода в {location}: {weather_data['forecast'][0]['description']}, Температура: {weather_data['forecast'][0]['temperature']}°C")
    else:
        print("Не удалось получить данные о погоде.")

# Определение аргументов по умолчанию
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'random_number_weather_dag',
    default_args=default_args,
    description='Генерация рандомного числа, запрос информации о погоде',
    schedule_interval='@once',
)

# Определение операторов
generate_random_number_task = BashOperator(
    task_id='generate_random_number',
    bash_command='echo $((RANDOM % 100))',
    dag=dag,
)

generate_and_square_task = PythonOperator(
    task_id='generate_and_square_random_number',
    python_callable=generate_and_square_random_number,
    dag=dag,
)

get_weather_task = PythonOperator(
    task_id='get_weather',
    python_callable=get_weather_and_print,
    op_kwargs={'location': 'Moscow'},
    dag=dag,
)

# Определение порядка выполнения задач
generate_random_number_task >> generate_and_square_task >> get_weather_task
