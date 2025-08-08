
# Обработка данных заказов Ozon

Этот скрипт обрабатывает данные заказов Ozon, преобразует их и загружает в базу данных PostgreSQL.

## Функционал

- Загрузка JSON-данных из файла
- Преобразование сырых данных в структурированный формат
- Создание таблиц в БД при их отсутствии
- Пакетная вставка данных с обработкой дубликатов
- Подробное логирование операций

## Требования

- Python 3.12+
- Зависимости (указаны в `requirements.txt`):
  - psycopg2
  - clickhouse-connect
  - pandas
  - sqlalchemy
  - python-dotenv

## Конфигурация

1. Создайте файл `.env` со следующими переменными:

```
POSTGRES_USER=ваш_пользователь
POSTGRES_PASSWORD=ваш_пароль
POSTGRES_DB=ваша_бд
POSTGRES_HOST=ваш_хост
POSTGRES_PORT=ваш_порт
```

и разместите его в папке с проектом

2. Разместите входной JSON-файл по пути `../data/test_task_ozon/ozon_orders.json`

## Структура проекта

```text
/project_root
│
├── /data
│ └── /test_task_ozon
│ └── ozon_orders.json # Исходные данные
│
├── /src
│ └── data_processor.py # Основной скрипт обработки
│
├── .env # Переменные окружения
├── app.log # Файл логов
├── docker-compose.yml # Конфигурация Docker
├── requirements.txt # Зависимости Python
└── README.md # Документация
```



## Конфигурация Docker

Файл `docker-compose.yml` для запуска PostgreSQL:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:17.5
    container_name: postgres-edu
    env_file:
      - .env
    ports:
      - "5432:5432"
    volumes:
      - ./postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

## Структура данных
Входные данные
Скрипт ожидает JSON с следующей структурой:

```json
{
  "order_id": "...",
  "status": "...",
  "date": "...",
  "amount": ...,
  "customer": {
    "id": "...",
    "region": "..."
  }
}
```

## Схема БД

Скрипт создает две таблицы:

**customers **
```
customer_id (PK) - Уникальный идентификатор клиента
region - Регион клиента
```

**orders**
```
order_id (PK) - Уникальный идентификатор заказа
status - Статус заказа
date - Временная метка заказа
amount - Сумма заказа
fk_customer_id (FK) - Ссылка на таблицу customers
```

## Использование

Установите зависимости:

```bash
pip install -r requirements.txt
```
Запустите контейнер PostgreSQL:


```bash
docker-compose up -d
```
Запустите скрипт:

```bash
python src/data_processor.py
```
## Логирование
Скрипт ведет логи в консоль и файл app.log в формате:


Выполняет пакетную вставку DataFrame в таблицу БД с обработкой конфликтов.

Параметры:

```
engine: SQLAlchemy engine

df: DataFrame для вставки

table_name: Имя целевой таблицы

conflict_columns: Столбцы, определяющие ограничение уникальности

schema: Схема БД (по умолчанию 'public')
```
Возвращает кортеж (попытки_вставки, успешные_вставки)

## Обработка ошибок

Скрипт включает комплексную обработку ошибок с блоками try-except и логированием для всех основных операций.

## Результат работы

Создает/обновляет таблицы в PostgreSQL

Ведет логи всех операций

Выводит количество вставленных записей


