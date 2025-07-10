# Weather stats app

Простое приложение для загрузки и преобразования данных с open-weather

## Setup
Для настройки базы данных запустите

```bash 
docker compose up -d
```

Для остановки используйте
```bash
docker compose stop
```

Все credentials и переменные для подключения находятся в файле `.env`
```bash
POSTGRES_DB=weather-stats
POSTGRES_USER=root
POSTGRES_PASSWORD=password
POSTGRES_PORT=5430
POSTGRES_HOST=localhost
```

Чтобы настроить virtual-environment запустите:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
```

## Запуск
Активируйте venv, если он ещё не активирован
```bash
source .venv/bin/activate
```

Скрипт выполняется следующей командой:
```bash
python main.py -lon LONGITUDE -lat LATITUDE [-df DATE_FROM] [-dt DATE_TO] [--csv] [--json]
```

Скрипт принимает различные параметры. 

Обязательные:
- `-lat` - ширина. Пример: 15.5
- `-lon` - долгота. Пример: 50.5

Дополнительные:
- `-df` начальная дата. Формат: YYYY-MM-DD
- `-dt` конечная дата. Формат: YYYY-MM-DD
- `--csv` - флаг, означающий выгрузку результата в файл csv
- `--json` - флаг, означающий выгрузку результата в файл json

Так же документацию по параметрам можно посмотреть через:
```bash
python main.py -h
```

Пример вызова:
```bash
python main.py -lon 50 -lat 80.123 -df 2025-06-20 -dt 2025-06-29 --csv
```
