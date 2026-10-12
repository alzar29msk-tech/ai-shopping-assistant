

| Элемент данных | Описание | Структура или тип данных | Значение |
| ----- | ----- | ----- | ----- |
| ai\_chat\_sessions  | Сессии чатов пользователей | session\_id  \+user\_id  \+device\_id  \+created\_at  \+updated\_at  |  |
| session\_id  | Уникальный ID чат-сессии пользователя | UUID  | f47ac10b-58cc-4372-a567-0e02b2c3d479  |
| user\_id | ID пользователя  | BIGINT  | 771029  |
| created\_at | Дата и время создания сессии (с таймзоной).  | TIMESTAMPZ  | 2026-10-08 14:15:22.123+03  |
| updated\_at | Дата и время обновления сессии (с таймзоной).  | TIMESTAMPZ | 2026-10-08 14:15:22.123+03  |
| ai\_recommendation\_requests  | Запросы пользователей для рекомендации товаров | request\_id  \+raw\_user\_input  \+ai\_raw\_response  \+request\_status  \+error\_message  |  |
| request\_id  | ID запроса  | UUID  | 9b1deb4d-5555-4444-3333-222211110000  |
| raw\_user\_input  | Исходный текст пользователя | TEXT  | Нужен комплект мебели в гостиную до 100к |
| ai\_raw\_response  | JSON-ответ от ИИ со списком товаров | TEXT  | {"status":"SUCCESS", "items":\[123, 125\]}  |
| request\_status  | Текущий статус обработки запроса к ИИ | enum  | Возможные варианты: 'PENDING',     \-- Запрос принят и отправлен в брокер 'COMPLETED',   \-- Корзина успешно сформирована ИИ 'FAILED'       \-- Ошибка ИИ или таймаут ‘TIMEOUT’  \-- таймаут  |
| error\_message  | Описание ошибки, если статус FAILED | TEXT  | LLM timeout after 15000ms  |
| retry\_count  | Число повторных отправок запроса в Kafka воркером | SMALLINT | По умолчанию 0  |
| created\_at | Время создания запроса | TIMESTAMPZ  | 2026-10-08 14:15:22.123+03  |
| completed\_at  | Время завершения обработки | TIMESTAMPZ  | 2026-10-08 14:15:22.123+03  |
| published\_at | Время подтверждения публикации в Kafka брокером  | TIMESTAMPTZ  | 2026-10-08 14:15:22.123+03  |
| ai\_recommended\_items  | Рекомендованные ии товары | product\_id  \+sku  \+item\_price \+currency |  |
| product\_id  | ID товара из каталога  | BIGINT  | 12345 |
| sku  | Складской артикул товара | VARCHAR(50)  | SOFA-SCAN-01-GREY  |
| item\_price | Цена товара | NUMERIC(12,2)  | 75000.00  |
| currency | Валюта | CHAR(3)  | RUB  |

