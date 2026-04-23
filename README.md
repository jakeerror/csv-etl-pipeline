**csv-etl-pipeline**  
ETL-пайплайн для загрузки, валидации и аналитической обработки данных из CSV-файлов с сохранением в PostgreSQL.  
**Что делает**  
- Читает CSV-файл и проверяет обязательные колонки  
- Валидирует и трансформирует каждую строку (типы, форматы, бизнес-правила)  
- Загружает чистые данные в PostgreSQL (дубликаты пропускаются)  
- Строит три аналитических отчёта с использованием оконных функций и CTE  
- Экспортирует результаты в CSV или JSON  
**Стек**  
| | |  
|-|-|  
| **Слой** | **Технологии** |   
| Язык | Python 3.11 |   
| БД | PostgreSQL 15 |   
| ORM | SQLAlchemy 2.0 |   
| Контейнеры | Docker, Docker Compose |   
| Тесты | Pytest, pytest-cov |   
| Линтинг | Ruff / flake8 |   
| CI/CD | GitLab CI |   
   
**Структура проекта**  
csv-etl-pipeline/  
 ├── src/  
 │   ├── etl/  
 │   │   ├── reader.py        # Чтение и первичная проверка CSV  
 │   │   ├── transformer.py   # Валидация и трансформация строк  
 │   │   └── loader.py        # Загрузка в PostgreSQL (batch upsert)  
 │   ├── db/  
 │   │   ├── connection.py    # SQLAlchemy engine, сессии  
 │   │   └── models.py        # ORM-модель Sale  
 │   └── analytics/  
 │       └── reports.py       # SQL-отчёты с оконными функциями  
 ├── tests/  
 │   ├── test_reader.py  
 │   └── test_transformer.py  
 ├── data/  
 │   └── sample.csv           # Пример входного файла  
 ├── reports/                 # Сюда сохраняются отчёты (gitignored)  
 ├── main.py  
 ├── docker-compose.yml  
 ├── Dockerfile  
 ├── .gitlab-ci.yml  
 ├── pyproject.toml  
 └── requirements.txt  
   
**Быстрый старт**  
**Через Docker Compose (рекомендуется)**  
git clone https://github.com/jakeerror/csv-etl-pipeline.git  
 cd csv-etl-pipeline  
   
 docker-compose up --build  
   
Пайплайн запустится автоматически на data/sample.csv. Отчёты появятся в папке reports/.  
**Локально**  
python -m venv venv  
 source venv/bin/activate      # Windows: venv\Scripts\activate  
 pip install -r requirements.txt  
   
 cp .env.example .env           # настройте DATABASE_URL  
   
 python main.py --file data/sample.csv --export csv  
 # или с экспортом в JSON:  
 python main.py --file data/sample.csv --export json  
   
**Формат входного CSV**  
Обязательные колонки:  
| | | |  
|-|-|-|  
| **Колонка** | **Тип** | **Описание** |   
| order_id | string | Уникальный идентификатор заказа |   
| product | string | Название товара |   
| category | string | Категория |   
| quantity | int > 0 | Количество |   
| price | decimal > 0 | Цена за единицу |   
| sale_date | YYYY-MM-DD | Дата продажи |   
| region | string | Регион |   
   
Строки с ошибками пропускаются с предупреждением в лог — пайплайн не падает.  
**Аналитические отчёты**  
**1. Топ-5 товаров по выручке в каждой категории**  
Использует RANK() OVER (PARTITION BY category ORDER BY revenue DESC) — ранжирование без схлопывания строк.  
**2. Динамика выручки по месяцам и регионам**  
Использует LAG() OVER (PARTITION BY region ORDER BY month) — разница с предыдущим месяцем для каждого региона.  
**3. Доля выручки по регионам**  
Использует CTE для подсчёта итогов и вычисления процентной доли каждого региона.  
**Тесты**  
pytest tests/ -v --cov=src --cov-report=term-missing  
   
Покрытие включает: валидацию трансформаций, обработку некорректных данных, проверку формата файла.  
**CI/CD**  
GitLab CI запускает при каждом push:  
1. lint — проверка кода через Ruff  
2. test — прогон Pytest с отчётом о покрытии  
