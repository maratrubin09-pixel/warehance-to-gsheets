# Warehance → Google Sheets Sync Agent

Автоматический агент для ежедневной синхронизации bill-details из **Warehance WMS** в **Google Таблицы** клиента.

```
Warehance API (bill details)     Python Agent            Google Sheet (tab "FBM")
─────────────────────────── → ─────────────────── → ───────────────────────────────
  49 raw charge rows             Group by order      Date | Order # | Tracking | ...
  (shipments, picking,           Sum per category    02.03 | 01-142.. | 943..   | 12.96
   storage, parcels...)          Add summary rows    Storage                    | 0.29
                                                     Return Processing          | 0
                                                     Return Labels              | 0
                                                     Total                      | 13.25
```

---

## Быстрый старт

### 1. Установка

```bash
git clone <your-repo-url>
cd warehance-to-gsheets
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Google Service Account

1. [Google Cloud Console](https://console.cloud.google.com/) → создайте проект
2. Включите **Google Sheets API** и **Google Drive API**
3. **IAM → Service Accounts** → создайте аккаунт → скачайте JSON-ключ
4. Положите файл в `config/service_account.json`
5. Откройте Google Таблицу клиента → **Share** → добавьте email сервисного аккаунта (`...@...iam.gserviceaccount.com`) как **Editor**

### 3. Warehance API ключ

Warehance Dashboard → Settings → API → скопируйте ключ

### 4. Создать .env

```bash
cp .env.example .env
```

Заполните:

```env
WAREHANCE_API_KEY=whc_your_key_here
GOOGLE_SPREADSHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
GOOGLE_SERVICE_ACCOUNT_FILE=config/service_account.json
GOOGLE_WORKSHEET_NAME=FBM
SYNC_DAYS_BACK=1
SYNC_MODE=append
```

> **SPREADSHEET_ID** — из URL таблицы:
> `https://docs.google.com/spreadsheets/d/`**1BxiM...**`/edit`

### 5. Запуск

```bash
# Тест с локальным CSV (без API)
python agent.py --csv path/to/bill-details.csv

# Из API, последний день
python agent.py

# Из API, последние 7 дней
python agent.py --days 7

# Демон — ежедневно в 06:00 UTC
python agent.py --schedule

# Демон — своё время
python agent.py --schedule --time 09:00
```

---

## Что делает трансформер

Из сырых строк CSV / API (каждая строка = один charge):

| Amount | Charge Category | Order Number | Tracking Number | ... |
|--------|----------------|--------------|-----------------|-----|
| 8.96 | shipments | 01-14200-99349 | 943463... | |
| 1.50 | picking | 01-14200-99349 | | |
| 0.30 | picking | 01-14200-99349 | | |
| 1.00 | shipment_parcels | 01-14200-99349 | 943463... | |
| 0.00 | storage | | | |

Получаем таблицу для клиента:

| Date | Order Number | Tracking number | Storage/Returns | Shipping cost | FBM fee | Package cost | Total |
|------|-------------|-----------------|-----------------|---------------|---------|-------------|-------|
| 02.03 | 01-14200-99349 | 943463... | | 8.96 | 3.0 | 1.0 | 12.96 |
| | Storage | | 0.0 | | | | 0.0 |
| | Return Processing Charges | | 0.0 | | | | 0.0 |
| | Return Labels Charges | | 0.0 | | | | 0.0 |
| | **Total** | | | | | | **24.92** |

---

## Варианты деплоя

### A) Локально через cron (для начала)

```bash
chmod +x setup_cron.sh
./setup_cron.sh
```

### B) GitHub Actions (бесплатно, рекомендуется)

1. Запушьте в приватный GitHub репозиторий
2. Settings → Secrets → добавьте:

| Secret | Значение |
|--------|----------|
| `WAREHANCE_API_KEY` | Ваш API-ключ |
| `GOOGLE_SPREADSHEET_ID` | ID таблицы |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Содержимое `service_account.json` |

3. Workflow уже готов: `.github/workflows/daily-sync.yml`

### C) Docker на VPS

```bash
docker build -t warehance-sync .

# Одноразовый запуск
docker run --env-file .env -v $(pwd)/config:/app/config warehance-sync

# Демон
docker run -d --restart unless-stopped \
  --env-file .env -v $(pwd)/config:/app/config \
  --name warehance-sync \
  warehance-sync --schedule
```

---

## Структура проекта

```
warehance-to-gsheets/
├── agent.py                 # Точка входа, CLI, планировщик
├── warehance_client.py      # Клиент Warehance API + CSV fallback
├── sheets_writer.py         # Запись в Google Sheets
├── transformer.py           # CSV → сводная таблица для клиента
├── requirements.txt
├── .env.example
├── .gitignore
├── Dockerfile
├── setup_cron.sh
├── config/
│   └── service_account.json   # (не коммитить!)
├── logs/
│   └── sync.log
└── .github/workflows/
    └── daily-sync.yml
```

---

## Настройка маппинга категорий

Если у вас в Warehance другие названия категорий, отредактируйте в `transformer.py`:

```python
SHIPPING_CATEGORIES = {"shipments"}
PICKING_CATEGORIES = {"picking"}
PACKAGE_CATEGORIES = {"shipment_parcels"}
STORAGE_CATEGORIES = {"storage"}
RETURN_PROCESSING_CATEGORIES = {"returns", "return_processing"}
RETURN_LABEL_CATEGORIES = {"return_labels", "return_shipments"}
```

---

## Troubleshooting

| Проблема | Решение |
|----------|---------|
| `Auth check failed` | Проверьте `WAREHANCE_API_KEY` |
| `Service account file not found` | Проверьте путь в `GOOGLE_SERVICE_ACCOUNT_FILE` |
| `403 Forbidden` (Sheets) | Расшарьте таблицу на email сервисного аккаунта |
| Пустые данные | Проверьте `SYNC_DAYS_BACK` — за этот период есть bill-details? |
| Числа с запятыми | Трансформер автоматически конвертирует `,` → `.` |

Логи: `logs/sync.log`
