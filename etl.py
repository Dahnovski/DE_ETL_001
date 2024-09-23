import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# loading data from CSV file
data = pd.read_csv('sales_data.csv')

# output data for checking output
print(data)

from datetime import datetime, timedelta

# 1. Преобразование даты в формат ISO 8601
data['order_date'] = pd.to_datetime(data['order_date'])

# 2. Рассчитаем общую стоимость
data['total_price'] = data['quantity'] * data['price']

# 3. Фильтрация данных: удалим заказы с отрицательной стоимостью или количеством
data = data[(data['total_price'] > 0) & (data['quantity'] > 0)]

# 4. Разделим на два набора данных: заказы за последние 30 дней и все остальные
cutoff_date = datetime.now() - timedelta(days=30)
recent_orders = data[data['order_date'] >= cutoff_date]
older_orders = data[data['order_date'] < cutoff_date]

# Проверим результаты
print("Recent orders:")
print(recent_orders)
print("\nOlder orders:")
print(older_orders)

# настройка подключения
user = 'my_username'
password = 'my_password'
host = 'localhost'
port = '5432'  # Стандартный порт для PostgreSQL, можешь указать свой
database = 'my_database'

# Формируем строку подключения
db_engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

# Создадим подключение
connection = db_engine.connect()

# SQL запрос для создания таблицы
create_table_query = """
CREATE TABLE IF NOT EXISTS sales (
    order_id INT PRIMARY KEY,
    order_date DATE,
    customer_id INT,
    product_id INT,
    quantity INT,
    price FLOAT,
    total_price FLOAT
);
"""

# Выполняем создание таблицы
connection.execute(create_table_query)

# Загрузим недавние заказы в таблицу 'sales'
recent_orders.to_sql('sales', con=db_engine, if_exists='append', index=False)

# Создаем подключение
with db_engine.connect() as connection:
    # Выполняем SQL-запрос для проверки данных
    query = "SELECT * FROM sales;"  # Здесь мы выбираем все данные из таблицы 'sales'
    
    # Используем pandas для удобного вывода данных
    data = pd.read_sql(query, connection)

    # Выводим данные
    print(data)

# Закроем соединение с базой данных
connection.close()

