# Tiny Relation Database

Невелика реляційна база даних, зроблена за зразком архітектури PostgreSQL: WAL (write-ahead log), checkpoint, транзакції, primary/foreign keys, підтримка багатьох підключень. Мова запитів — українська DSL (`отримати`, `додати`, `оновити`, `видалити`, `створити таблицю`).

---

## Що зроблено та думки

Підготовлено єдиний план розвитку проєкту з урахуванням швидкості, конкурентного доступу та відповідності ідеям PostgreSQL — щоб можна було використати це в дипломній роботі та послідовно реалізувати зміни.

**Конкурентність і безпека даних.** Зараз один клієнт може читати таблицю, поки інший її змінює або видаляє — це дає ризик пошкодження файлів (наприклад під час rewrite при UPDATE/DELETE) або неконсистентного результату read. Тому першим кроком заплановано **блокировки на рівні таблиці** (`asyncio.Lock` на таблицю): будь-яка операція з таблицею виконується під lock, читач і видалення не йдуть одночасно — результат читання завжди консистентний. Це дозволяє підтримувати багато підключень без гонок; паралельність зберігається між різними таблицями. Пізніше можна додати read-write lock для паралельних читання однієї таблиці.

**Швидкість.** Дані зараз у JSONL — повільний парсинг і великий об’єм на диску. Заплановано перехід на **MessagePack** з **length-prefixed** записами (4 байти довжини + payload): швидша серіалізація/десеріалізація, менший розмір, і головне — можливість читати запис за **байтовим offset**. Це потрібно для індексу по primary key: індекс зберігатиме offset у файлі, GET по ключу стане O(1) замість повного скану. UPDATE/DELETE по PK теж можна прискорити за рахунок роботи лише з потрібним записом.

**WAL і checkpoint.** Є один файл WAL і checkpoint-записи; немає окремого файлу типу `pg_control` і явного XID у записах. Заплановано: окремий файл **pg_control** з `last_checkpoint_lsn` для швидкого старту recovery; поле **transaction_id (XID)** у кожному WAL-записі; при checkpoint — запис у WAL і оновлення pg_control; періодичний checkpoint, команда `checkpoint` і checkpoint при shutdown. Так поведінка наближається до PostgreSQL і дає чітку точку відновлення.

**Транзакції.** План передбачає явні **BEGIN / COMMIT / ROLLBACK**: стан на підключення (in_transaction, XID, буфер команд), при COMMIT — виконання буфера і запис COMMIT у WAL, при ROLLBACK — скидання буфера. Recovery відтворює лише операції з відповідним COMMIT. Це дає зрозумілу модель для диплому і коректну роботу при багатьох клієнтах.

**Primary key, foreign keys, JOIN.** PK — у схему й парсер, валідація унікальності та індекс (PK → byte offset) для швидкого доступу. FK — перевірка посилань при INSERT/UPDATE і обробка ON DELETE (RESTRICT/CASCADE/SET NULL). JOIN — розширення GET (приєднати друга таблиця, умова ON), виконання через hash join або nested loop під блокировками обох таблиць.

**Індекси Hash та B-tree+.** Окрім індексу по primary key, плануються довільні індекси по колонках: **Hash** — для пошуку за рівністю (key → список offset), O(1) в середньому; **B-tree+** — упорядковане дерево для діапазонів (WHERE col &gt; value, BETWEEN) та ORDER BY. Синтаксис на кшталт «створити індекс ім’я по таблиця(колонка) hash/btree»; оптимізатор (або простий вибір у executor) використовує індекс при відповідному WHERE.

Порядок реалізації обрано так: спочатку блокировки (щоб усі подальші зміни були безпечні при конкурентному доступі), потім швидкий формат і PK (щоб індекс одразу опиратився на offset), далі WAL/checkpoint/транзакції, потім FK і JOIN.

---

## Todos (покроковий план)

Виконувати по черзі; кожен блок можна закривати після проходження тестів.

### Фаза 0: Підготовка

- Додати залежність `msgpack` (requirements.txt або pyproject.toml).
- Реалізувати блокировки на таблицю в `TableStorage`: `_table_locks`, `_get_table_lock(table)`, обгорнути `create_table`, `add`, `get`, `update`, `delete` у `async with self._get_table_lock(table)`.
- E2E-тест: два клієнти — один читає в циклі, інший видаляє/додає; перевірити відсутність exception і консистентність read.

### Фаза 1: Швидкий формат зберігання

- Допоміжні функції: `_serialize_row` (4 байти length + msgpack), `_deserialize_row` у `table_storage.py`.
- Новий шлях даних: `{table}.data.bin`, методи читання/запису length-prefixed msgpack.
- Переключити `add` / `get` / `update` / `delete` на бінарний формат; переконатися, що тести проходять.
- Опційно: скрипт міграції `scripts/migrate_jsonl_to_msgpack.py` для існуючих `.data.jsonl`.

### Фаза 2: Primary key та індекс

- Схема та парсер: поле `primary_key` у CreateTableCommand і в `{table}.schema.json`, токени/правило PRIMARY KEY.
- Валідація: унікальність при `add`, заборона зміни PK при `update`.
- Індекс: файл `{table}.pk.idx` (PK_value → byte offset); оновлення при add/update/delete; GET по PK через індекс (O(1)).
- Оптимізація update/delete по PK (робота по offset, без повного скану).

### Фаза 2b: Індекси Hash та B-tree+

- **Hash-індекс:** структура key → список byte offset (або row id); файл наприклад `{table}.idx.{column}.hash` (або один файл з метаданими по індексах). Синтаксис: створити індекс по колонці (наприклад «створити індекс idx_name по users(email) hash»). При GET з WHERE col = value перевіряти наявність hash-індексу по col і виконувати пошук через нього; оновлювати індекс при add/update/delete по цій колонці.
- **B-tree+ індекс:** упорядкована структура (дерево) для діапазонних запитів та сортування. Зберігання: на диску (сторінки дерева) або серіалізоване дерево у файлі `{table}.idx.{column}.btree`. Синтаксис: «створити індекс idx_name по users(created_at) btree». Підтримка WHERE col &gt; / &lt; / BETWEEN та ORDER BY col — обхід дерева замість повного скану; оновлення індексу при зміні значень колонки.
- Парсер/лексер: нові команди або розширення CREATE (створити індекс, тип hash | btree); збереження метаданих індексів (наприклад у schema або окремому каталозі індексів).
- Executor/storage: при створенні індексу — побудова по поточних даних таблиці; при add/update/delete — оновлення всіх індексів по змінених колонках; при get — вибір індексу (hash для =, btree для діапазону/ORDER BY) якщо є підходящий.

### Фаза 3: WAL і checkpoint

- Файл pg_control (control.json): `last_checkpoint_lsn`, `timeline_id`; запис при checkpoint (atomic write).
- Поле `transaction_id` (XID) у WALEntry, генератор XID, передача XID у `log_operation` з server.
- Recovery: старт з pg_control, fallback на пошук checkpoint у WAL; REDO лише після LSN checkpoint.
- Періодичний checkpoint (фонова задача), команда клієнта `checkpoint`, checkpoint при shutdown.

### Фаза 4: Транзакції (BEGIN / COMMIT / ROLLBACK)

- Парсер/лексер: команди `почати`/BEGIN, `зберегти`/COMMIT, `скасувати`/ROLLBACK; типи команд у command.py та executor.
- Стан на підключення в server: in_transaction, transaction_xid, буфер команд; логіка BEGIN → буфер → COMMIT/ROLLBACK.
- Recovery: відтворювати лише операції з записом COMMIT для XID; ігнорувати ABORT та незавершені транзакції.

### Фаза 5: Foreign keys

- Модель ForeignKeySpec, поле у CreateTableCommand і schema; парсер REFERENCES, ON DELETE.
- Валідація при add/update (наявність рядка в ref_table); при delete/update у ref_table — RESTRICT/CASCADE/SET NULL; підтримка NULL для SET NULL.

### Фаза 6: JOIN

- Парсер: розширення GET — `приєднати` друга таблиця, умова ON; GetCommand: join_table, join_type, join_on.
- Executor: читання двох таблиць під locks, hash join або nested loop, повернення об’єднаних рядків.

---

## Покроковий гайд (step-by-step)

Деталізовані кроки для кожної фази; кожен крок перевіряти тестами перед переходом далі.

**Фаза 0.** Крок 0.1 — додати `msgpack` у requirements/pyproject. Крок 0.2 — у `TableStorage`: `_table_locks: dict[str, asyncio.Lock]`, `_get_table_lock(table)`, обгорнути `create_table`, `add`, `get`, `update`, `delete` у `async with self._get_table_lock(table)`; переконатися, що test_multiple_clients і інші тести проходять. Крок 0.3 — e2e-тест: два клієнти (один у циклі читає, інший видаляє/додає), перевірити відсутність exception і консистентність read.

**Фаза 1.** Крок 1.1 — у `table_storage.py`: `_serialize_row(row)` (4 байти length big-endian + msgpack.packb), `_deserialize_row(data)`. Крок 1.2 — `_data_path_bin(table)` → `{table}.data.bin`, методи читання/запису length-prefixed msgpack. Крок 1.3 — переключити add/get/update/delete на бінарний формат, запустити тести. Крок 1.4 (опційно) — скрипт міграції `*.data.jsonl` → `*.data.bin`.

**Фаза 2.** Крок 2.1 — CreateTableCommand.primary_key, парсер PRIMARY KEY, збереження в schema. Крок 2.2 — валідація унікальності при add, заборона зміни PK при update. Крок 2.3 — файл `{table}.pk.idx` (PK_value → byte offset), оновлення при add/update/delete, get по PK через індекс. Крок 2.4 — оптимізація update/delete по PK (робота по offset).

**Фаза 2b.** Крок 2b.1 — Hash-індекс (key → [offset], файл `{table}.idx.{column}.hash`, команда «створити індекс … hash», побудова та оновлення, використання при WHERE col = value). Крок 2b.2 — B-tree+ індекс (дерево, файл `.btree`, діапазони та ORDER BY). Крок 2b.3 — парсер CREATE INDEX (ім’я, таблиця, колонка, hash|btree), реєстр індексів, вибір індексу в executor.

**Фаза 3.** Крок 3.1 — pg_control (control.json): last_checkpoint_lsn, timeline_id; atomic write при checkpoint. Крок 3.2 — WALEntry.transaction_id (XID), генератор XID, передача з server у log_operation. Крок 3.3 — recovery: старт з pg_control, fallback на пошук checkpoint у WAL, REDO після LSN. Крок 3.4 — періодичний checkpoint, команда `checkpoint`, checkpoint при shutdown.

**Фаза 4.** Крок 4.1 — парсер/лексер: почати/BEGIN, зберегти/COMMIT, скасувати/ROLLBACK; command.py, executor. Крок 4.2 — server: in_transaction, transaction_xid, буфер команд; BEGIN → буфер → COMMIT/ROLLBACK. Крок 4.3 — recovery лише по COMMIT для XID; ігнорувати ABORT та незавершені транзакції.

**Фаза 5.** Крок 5.1 — ForeignKeySpec, CreateTableCommand + schema, парсер REFERENCES, ON DELETE. Крок 5.2 — валідація при add/update; при delete/update у ref_table — RESTRICT/CASCADE/SET NULL; NULL для SET NULL.

**Фаза 6.** Крок 6.1 — парсер GET з приєднати, умова ON; GetCommand (join_table, join_type, join_on). Крок 6.2 — executor: читання двох таблиць під locks (по черзі, щоб уникнути deadlock), hash join або nested loop, повернення об’єднаних рядків.

---

## Що переробити під швидкість і багато підключень

- **Блокировки** — обов’язково першими (Фаза 0); інакше тести на конкурентність і продакшн нестабільні. Для максимальної швидкості читання при багатьох клієнтах пізніше замінити один lock на таблицю на read-write lock.
- **Формат даних** — перехід на MessagePack і length-prefix одразу після lock (Фаза 1), щоб індекс PK міг зберігати byte offset.
- **Індекс PK** — без нього GET по ключу O(n); з індексом — O(1) + одна read. UPDATE/DELETE по PK теж прискорити через offset.
- **WAL** — не блокує паралельність клієнтів (лог у одну чергу); лише консистентність і recovery. Checkpoint у фоні не тримає довгий lock таблиць; достатньо fsync і запису pg_control.

