# sales-etl-pipeline

ETL-пайплайн для загрузки, валидации и аналитической обработки данных о продажах из CSV-файлов с сохранением в PostgreSQL.

---

## Стек

- **Python 3.11** — язык
- **PostgreSQL 15** — хранение данных
- **SQLAlchemy 2.0** — ORM и работа с БД
- **Docker / Docker Compose** — контейнеризация
- **Pytest + pytest-cov** — тесты и покрытие
- **Ruff** — линтинг
- **GitLab CI** — автоматический прогон тестов

---

## Что делает

1. Читает CSV-файл, проверяет наличие обязательных колонок
2. Валидирует каждую строку — пропускает невалидные с предупреждением в лог
3. Загружает чистые данные в PostgreSQL (дубликаты игнорируются)
4. Строит три аналитических отчёта с оконными функциями и CTE
5. Экспортирует результаты в CSV или JSON

---

## Структура проекта

```
sales-etl-pipeline/
├── src/
│   ├── etl/
│   │   ├── reader.py         # Чтение и проверка CSV
│   │   ├── transformer.py    # Валидация и трансформация строк
│   │   └── loader.py         # Загрузка в PostgreSQL (batch upsert)
│   ├── db/
│   │   ├── connection.py     # SQLAlchemy engine, сессии
│   │   └── models.py         # ORM-модель Sale
│   └── analytics/
│       └── reports.py        # SQL-отчёты с оконными функциями
├── tests/
│   ├── test_reader.py
│   └── test_transformer.py
├── data/
│   └── sample.csv            # Пример входного файла
├── reports/                  # Сюда сохраняются отчёты (gitignored)
├── main.py
├── docker-compose.yml
├── Dockerfile
├── .gitlab-ci.yml
├── pyproject.toml
└── requirements.txt
```

---

## Быстрый старт

### Через Docker Compose (рекомендуется)

```bash
git clone https://github.com/your-username/sales-etl-pipeline.git
cd sales-etl-pipeline
```

```bash
docker-compose up --build
```

Пайплайн запустится автоматически на `data/sample.csv`. Отчёты появятся в папке `reports/`.

### Локально

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

```bash
cp .env.example .env
```

Откройте `.env` и укажите строку подключения к БД:

```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/etl_db
```

```bash
python main.py --file data/sample.csv --export csv
```

Экспорт в JSON:

```bash
python main.py --file data/sample.csv --export json
```

---

## Формат входного CSV

Обязательные колонки:

| Колонка | Тип | Описание |
| --- | --- | --- |
| `order_id` | string | Уникальный идентификатор заказа |
| `product` | string | Название товара |
| `category` | string | Категория |
| `quantity` | int > 0 | Количество |
| `price` | decimal > 0 | Цена за единицу |
| `sale_date` | YYYY-MM-DD | Дата продажи |
| `region` | string | Регион |

Строки с ошибками пропускаются с предупреждением в лог — пайплайн не останавливается.

---

## Аналитические отчёты

**1. Топ-5 товаров по выручке в каждой категории**

Использует `RANK() OVER (PARTITION BY category ORDER BY revenue DESC)` — ранжирование без схлопывания строк.

**2. Динамика выручки по месяцам и регионам**

Использует `LAG() OVER (PARTITION BY region ORDER BY month)` — разница с предыдущим месяцем для каждого региона.

**3. Доля выручки по регионам**

Использует CTE для вычисления итоговой суммы и процентной доли каждого региона.

---

## Тесты

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

12 тестов покрывают: валидацию трансформаций, обработку некорректных данных, проверку формата файла.

---

## CI/CD

GitLab CI запускает при каждом `push` два этапа:

1. **lint** — проверка кода через Ruff
2. **test** — прогон Pytest с отчётом о покрытии
