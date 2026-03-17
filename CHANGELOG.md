# Changelog — warehance-to-gsheets v2.1

**Дата:** 13.03.2026

---

## v2.1.2 — SOLMAR 257 Payments fix (17.03.2026)

### Полная перезапись Payments таба для клиента 257 (SOLMAR)

**Проблемы, которые были:**
1. Два Total ряда (row 2 с формулами, row 62 с хардкодом -76219.91)
2. Старые данные (01/04–03/12): одна строка в день, без разбивки по категориям
3. Новые данные (03/13+): имели комментарии Shopify/Storage, но несовместимый формат
4. Balance (колонка D) пустой для новых строк
5. Отсутствовала дата 01/02/26 (первый биллинговый день)

**Что сделано (Variant B — полная перезапись):**
1. Удалены все старые данные (59 строк) + второй Total row
2. Перезаписаны **142 строки** с разбивкой по категориям (Shopify, Storage, Returns, Return Labels) за все 64 дня (01/02–03/16)
3. Каждая строка имеет формулу Balance `=B{row}-C{row}`
4. Total row обновлён на open-ended формулы: `=SUM(B3:B)`, `=SUM(C3:C)`, `=SUM(D3:D)`
5. Добавлена дата 01/02/26 ($1,672.75) — ранее отсутствовала

**Итого:** Paid = $41,121.80 | Balance = -$41,121.80

### `sheets_writer.py` — обновления

1. **`write_payment()`** — теперь пишет формулу Balance `=B{row}-C{row}` в каждую новую строку (ранее оставлял пустым)
2. **`clear_and_init_payments()`** — Total row Balance изменён с `=B2-C2` на `=SUM(D3:D)` для консистентности с row-level формулами

---

## v2.1 — Исправления (13.03.2026)

### `transformer.py` — v2.1

1. **Split-day pick fee detection** — если заказ был spicked вчера, а отгружен сегодня, pick_fee будет 0 в сегодняшнем билле. Transformer теперь возвращает `missing_pick_orders` — список таких заказов для разрешения агентом.

2. **Улучшенная логика аномалий package cost:**
   - `Custom` упаковка → `package_cost=0` — это нормально, не помечается как аномалия
   - Клиент 154: `pick_fee=1.50` (один товар) + `package_cost=0` — нормально, не аномалия

3. **Клиент 257 (SOLMAR) — разбивка платежей:**
   - Возвращает `payments_rows` (множественные строки) вместо одной строки
   - Категории: "Shopify" (заказы), "Storage", "Return Labels", "Returns"
   - Каждая категория — отдельная строка в Payments с комментарием

4. **`category_totals`** — новый ключ в результате: `storage`, `return_processing`, `return_labels`, `orders_total` для внешнего использования

### `agent.py` — v2.1

1. **Split-day pick fee resolution:**
   - Если transformer нашёл заказы с `pick_fee=0` (но с shipping), agent запрашивает билл **за предыдущий день**
   - Из предыдущего билла извлекаются pick fee только для нужных заказов
   - Pick fee мержатся в результат: обновляются `report_rows`, пересчитывается `grand_total`, убираются resolved аномалии
   - Работает автоматически, не требует ручного вмешательства

2. **Клиент 257 (SOLMAR) — многострочные платежи:**
   - Если `payments_rows` есть (клиент 257), пишет несколько строк в Payments
   - Каждая строка с комментарием: Storage, Shopify, Return Labels, Returns
   - Для остальных клиентов — стандартная одна строка

3. **Передаётся `client_number`** в `transform_bill_details()` для корректной работы специальных правил (клиент 154, клиент 257)

---

### Payments — поддержка ручных операций

**Новая структура Payments таба:**

| Row | A: Date | B: Deposit | C: Paid | D: Balance | E: Comments | F: Customer info |
|-----|---------|------------|---------|------------|-------------|------------------|
| 1   | _header_ | _header_ | _header_ | _header_ | _header_ | _header_ |
| 2   | **Total** | `=SUM(B3:B)` | `=SUM(C3:C)` | `=B2-C2` | | |
| 3+  | Данные (авто + ручные) | | | | | |

- **Balance** в Total row = Депозиты - Расходы (обновляется автоматически)
- Скрипт всегда **дописывает в конец**, не трогает ручные строки
- Ручные операции можно вносить в любую строку 3+:
  - **Депозит**: Date + Deposit (колонка B) + Comments
  - **Кастомный чардж**: Date + Paid (колонка C) + Comments
  - **Коррекция**: Date + отрицательное значение в Paid + Comments

### Миграция существующих таблиц: `migrate_payments.py`

Добавляет Total row (строка 2) во все существующие Payments табы.

```bash
python migrate_payments.py --dry-run       # Превью без изменений
python migrate_payments.py                  # Миграция всех клиентов
python migrate_payments.py --client 257     # Один клиент
```

### Важно для `sheets_writer.py`

`write_payment()` теперь принимает `comment=""`. Обновить метод:

```python
def write_payment(self, spreadsheet_id, tab_name, date, paid_amount, comment=""):
    """Append a payment row to the Payments tab.
    
    Layout: Row 1 = headers, Row 2 = Total (formulas), Row 3+ = data.
    Always appends AFTER last row (safe for manual entries).
    """
    ws = self.client.open_by_key(spreadsheet_id).worksheet(tab_name)
    all_vals = ws.get_all_values()
    next_row = len(all_vals) + 1
    
    # Build row: Date | Deposit (empty) | Paid | Balance (empty) | Comments
    row = [date, "", paid_amount, "", comment, ""]
    ws.update(f"A{next_row}:F{next_row}", [row], value_input_option="USER_ENTERED")
```

**Ключевое:** Balance не пишется в каждую строку — он считается формулой в Total row (D2).

---

## v2.0 — Основной релиз (13.03.2026)

## Новые файлы

### `brand_config.py`
Единый файл конфигурации бренда. Все цвета, email'ы, ширины колонок и billing profile ID теперь в одном месте. Больше не нужно менять цвета в нескольких файлах.

### `client_discovery.py`
Автоматическое обнаружение новых клиентов в Warehance.

**Как работает:**
1. Запрашивает `GET /v1/clients` из Warehance API
2. Сравнивает с `clients.json` — находит новых по `warehance_id`
3. Для каждого нового клиента:
   - Присваивает следующий свободный номер
   - Создаёт Google Sheet с AllReports + Payments (брендовое оформление)
   - Расшаривает на office@fastprepusa.com + bwmodnick@gmail.com
   - Назначает стандартный billing profile (или спецпрофайл для 001/154)
   - Записывает в `clients.json`
   - Обновляет мастер-Dashboard
   - Шлёт Telegram-уведомление

**Запуск:**
```bash
python agent.py --discover          # Только discovery
python agent.py                     # Discovery + sync (по умолчанию)
python agent.py --no-discovery      # Sync без discovery
```

**Переменная окружения:** `ENABLE_CLIENT_DISCOVERY=true` (по умолчанию)

### `business_pnl.py`
Ежемесячная таблица прибыли/убытков всего бизнеса.

**Структура листа "Business P&L":**
```
                        Jan    Feb    Mar   ...   Dec    YTD
REVENUE
  Pick & Pack           [auto]
  Storage               [auto]
  Return Processing     [auto]
  Return Labels         [auto]
  Packaging Revenue     [auto]
  Shipping Revenue      [auto]
  Total Revenue         [auto]

COST OF GOODS SOLD
  Shipping (Carriers)   [auto]
  Packaging (Materials) [auto]
  Total COGS            [auto]

GROSS PROFIT            [auto]
Gross Margin %          [auto]

OPERATING EXPENSES
  Rent                  [вручную]
  Utilities             [вручную]
  Internet & Phone      [вручную]
  Salaries & Wages      [вручную]
  Advertising           [вручную]
  Software & Tools      [вручную]
  Insurance             [вручную]
  Other Expenses        [вручную]
  Total OpEx            [формула]

NET PROFIT              [формула]
Net Margin %            [формула]
```

**Верхняя часть** (Revenue, COGS) — заполняется автоматически формулами из вкладки "Data".
**Нижняя часть** (OpEx) — вносится вручную раз в месяц.

**Запуск:**
```bash
python agent.py --setup-business-pnl
```

---

## Изменённые файлы

### `transformer.py` — v2
**Новое:**
- Колонка **Packaging Type** — извлекается из Description/Charge Rule Name в CSV
- Логирование **неизвестных категорий** — если Warehance добавит новый тип заряда, он не пропадёт молча, а попадёт в WARNING лог
- Новый порядок колонок AllReports: Date | Order Number | Tracking | Pick&Pack fee | Packaging Type | Packaging Cost | Shipping cost | Total

**Обратная совместимость:** Storage, Return Processing, Return Labels по-прежнему идут отдельными строками внизу (дневные расходы, не по заказам).

### `write_pnl.py` — v2
**Новые колонки P&L (18 вместо 16):**
- Packaging Revenue + Packaging Cost (COGS) + **Packaging Profit**
- Shipping Revenue + Shipping Cost (COGS) + **Shipping Profit**
- Total Revenue + Total COGS + Gross Profit + Gross Margin %

**Расчёт:**
- Packaging Profit = Packaging Revenue (что платит клиент) - Packaging Cost (наша закупочная из packaging_costs.json)
- Shipping Profit = Shipping Revenue (что платит клиент) - Shipping Cost (что платим перевозчику, из Shipments API)

Теперь использует цвета из `brand_config.py` вместо хардкода.

### `agent.py` — v2
**Новое:**
1. **Auto-discovery** — при каждом запуске проверяет новых клиентов в Warehance (можно отключить: `--no-discovery` или `ENABLE_CLIENT_DISCOVERY=false`)
2. **Дедупликация AllReports** — проверяет, есть ли уже данные за эту дату, не дублирует
3. **Backup через .env** — `ENABLE_GDRIVE_BACKUP=true/false` вместо `if False and backup`
4. **Новые CLI-команды:**
   - `--discover` — только обнаружение новых клиентов
   - `--setup-business-pnl` — создание таблицы Business P&L
   - `--no-discovery` — пропустить авто-обнаружение
5. **Расширенная валидация** — проверяет billing_profile_id и warehance_id
6. **Перезагрузка clients.json в scheduler** — каждый запуск по расписанию читает свежий конфиг (вдруг discovery добавил клиентов)

---

## Исправленные баги

| # | Баг | Файл | Что сделано |
|---|-----|------|-------------|
| 1 | Бэкап выключен хардкодом `if False` | `agent.py` | Заменён на переменную `ENABLE_GDRIVE_BACKUP` в .env |
| 2 | AllReports не проверяет дубли | `agent.py` | Добавлена проверка: если дата уже есть в таблице, запись пропускается |
| 3 | Неизвестные категории молча игнорируются | `transformer.py` | Добавлен WARNING лог с перечислением пропущенных категорий |
| 4 | Бренд-цвета дублируются в 2 файлах | Весь проект | Вынесены в `brand_config.py` |
| 5 | Валидация не проверяла billing_profile_id | `agent.py` | Добавлена проверка |

---

## Переменные окружения (.env) — новые

```env
ENABLE_CLIENT_DISCOVERY=true         # Авто-обнаружение новых клиентов
ENABLE_GDRIVE_BACKUP=false           # Бэкап CSV на Google Drive
```

---

## Порядок деплоя

1. Скопировать новые файлы: `brand_config.py`, `client_discovery.py`, `business_pnl.py`, `migrate_payments.py`
2. Заменить изменённые файлы: `agent.py`, `transformer.py`, `write_pnl.py`, `create_all_sheets.py`
3. Обновить `sheets_writer.py` (см. инструкцию выше)
4. Добавить в `.env`: `ENABLE_CLIENT_DISCOVERY=true`
5. **Миграция Payments** (один раз): `python migrate_payments.py`
6. Запустить: `python agent.py --setup-business-pnl` (один раз, для создания вкладки)
7. Запустить обычную синхронизацию: `python agent.py`

---

## Что было доделано (deployment)

1. **`sheets_writer.py`** — обновлён (v2.1):
   - `write_allreports()` — новый порядок колонок (8 колонок, Packaging Type)
   - `write_payment()` — добавлен параметр `comment=""`, Balance не пишется в каждую строку
   - `clear_and_init_payments()` — Total row с формулами `=SUM(B3:B)`, `=SUM(C3:C)`, `=B2-C2`
2. **Миграция Payments** — выполнена для всех 13 клиентов (Total row добавлен)
3. **Dashboard формулы** — обновлены: `IMPORTRANGE("...", "Payments!D2")` для всех 13 клиентов
   - ⚠️ При первом открытии Dashboard нужно нажать "Allow access" для каждого IMPORTRANGE (один раз)
4. **Осталось:** GitHub Actions — заменить хардкод зависимостей на `pip install -r requirements.txt`.

---

## Структура файлов (обновлённая)

```
agent.py                  ← Оркестратор (обновлён)
brand_config.py           ← НОВЫЙ: единый конфиг бренда
client_discovery.py       ← НОВЫЙ: авто-обнаружение клиентов
business_pnl.py           ← НОВЫЙ: ежемесячный P&L бизнеса
transformer.py            ← Трансформация данных (обновлён)
write_pnl.py              ← P&L по клиентам (обновлён)
warehance_client.py       ← API-клиент (без изменений)
sheets_writer.py          ← Запись в Sheets (НУЖНО обновить)
telegram_notifier.py      ← Алерты (без изменений)
backfill.py               ← Backfill (НУЖНО обновить)
migrate_payments.py       ← НОВЫЙ: миграция Payments (добавление Total row)
backfill_all.py           ← Пакетный backfill (без изменений)
create_all_sheets.py      ← Создание таблиц (заменяется client_discovery.py)
gdrive_backup.py          ← Бэкап (без изменений)
clients.json              ← Конфиг клиентов (авто-обновляется discovery)
packaging_costs.json      ← Цены упаковки (без изменений)
```
