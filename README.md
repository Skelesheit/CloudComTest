# Тестовое задание в CloudCom

Тестовое ETL-решение на базе Apache Airflow. Реализует три пайплайна: загрузку данных в PostgreSQL, межтабличную перегрузку и ежедневную выгрузку в S3-совместимое хранилище (MinIO). Вся инфраструктура поднимается через Docker Compose.

## Стек

- **Python 3.12**
- **Apache Airflow 3.1.2** (CeleryExecutor)
- **PostgreSQL 16** — основная БД
- **Redis** — брокер для Celery
- **MinIO** — S3-совместимое объектное хранилище
- **pgAdmin** — UI для работы с PostgreSQL
- **SQLAlchemy** - ORM для базы данных 
- **Docker / Docker Compose** - Развёртывание
- **Celery worker** - Воркеры для работы DAG

## Данные

```
Airflow DAGs
   ├── PostgreSQL  →  t1, t2
   └── MinIO (S3)  →  YYYY-MM-DD/data.json  
```

Airflow выступает оркестратором: управляет загрузкой, трансформацией и выгрузкой данных. Redis используется как брокер задач для CeleryExecutor.

<br>
Внимание ! Необходимо поместить `data.csv` в `dags/data/data.csv`, был взят датасет с <https://www.kaggle.com/datasets/rai220/russian-cyrillic-names-and-sex>

## DAG'и

| DAG         | Расписание | Описание |
|-------------|------------|----------|
| `csv_to_t1` | ручной запуск | Загружает первые 100 строк датасета в таблицу `t1` |
| `t1_to_t2`  | ежедневно | Полностью очищает `t2` и переносит все данные из `t1` в одной транзакции |
| `t1_to_s3`  | ежедневно | Выгружает данные из `t1` за текущий `data_interval` в MinIO в формате JSON; если записей нет — таск завершается в состоянии `skipped` |

Выгрузка в S3 сохраняется по пути:
```
default-bucket/
  YYYY-MM-DD/
    data.json
```

## Быстрый старт

### 1. Переменные окружения

Все переменные уже готовы в `.docker.example.env` (проект с ними запустится).
Переписывать переменные не обязательно.


Основные переменные:

```env
# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123
S3_BUCKET=default-bucket

# Airflow
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=123
```
При изменении конфигурации БД необходимо соответственно поменять `DB_URL`

### 2. Запуск

```bash
docker compose -f docker-airflow.yml up -d
```

Поднимаются сервисы: `postgres`, `redis`, `minio`, `pgadmin`, `airflow-apiserver`, `airflow-scheduler`, `airflow-worker`, `airflow-dag-processor`, `airflow-triggerer`.

### 3. Инициализация S3-бакета

После старта, если начальный бакет `default-bucket` не был создан, выполните однократную инициализацию:

```bash
docker compose -f docker-airflow.yml run minio-init-bucket
```

Создаёт бакет `default-bucket`. 
Иначе: если нужно сделать вручную — нужно зайти в MinIO Console (`http://localhost:9001`) и создать бакет с тем же именем.

### 4. Подключение S3 в Airflow

Нужно перейти в **Admin → Connections → Add Connection** и заполнить:

| Поле | Значение |
|------|----------|
| Conn Id | `aws_default` |
| Conn Type | Amazon Web Services |
| AWS Access Key ID | `admin` |
| AWS Secret Access Key | `password123` |
| Extra | `{"endpoint_url": "http://minio:9000"}` |

Поле `Extra` обязательно — без него Airflow будет пытаться обратиться к AWS вместо локального MinIO.

### 5. Просмотр и тесты

Просмотр данных доступно по следующим UI:

## Доступные UI

| Сервис | Адрес | Логин / Пароль |
|--------|-------|----------------|
| Airflow Web UI | http://localhost:8080 | `airflow` / `123` |
| pgAdmin | http://localhost:5050 | `admin@gmail.com` / `admin` |
| MinIO Console | http://localhost:9001 | `admin` / `password123` |


### Регистрация сервера postgres

| Поле      | Окно | Значение |
|-----------|------|----------|
 | Name      | General| airflow|
| Host name | Connection | postgres|
| Port      | Connection | 5432 |
| Username  | Connection | airflow |
| Password  | Connection | airflow |

Остальные поля трогать не обязательно.

После подключения в схеме `public` будут доступны таблицы `t1` и `t2`.



## Проверка работы

**DAG 1 — csv_to_t1**: запусти вручную, проверь через pgAdmin что `t1` заполнена.

**DAG 2 — t1_to_t2**: запусти вручную, проверь что `t2` содержит те же данные что `t1`.

**DAG 3 — t1_to_s3**: запусти вручную, в MinIO Console убедись что в бакете `default-bucket` появилась директория с датой и файлом `data.json` внутри.

