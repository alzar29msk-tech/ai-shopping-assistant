# **ТЗ: AI-чат для подбора товаров маркетплейса**

## **1\. Общее описание**

### **1.1 Название**

AI-чат для подбора товаров маркетплейса: API отправки сообщения в чат (с созданием чата), API получения статуса запроса, WebSocket-канал уведомлений и Kafka-топики обмена с AI Service.

### **1.2 Цель**

Дать пользователю возможность описать потребность текстом (например, «нужен набор для кемпинга на двоих») и получить подборку товаров каталога, не удерживая открытое HTTP-соединение. Время ответа HTTP-методов не должно зависеть от времени работы ИИ.

### **1.3 Контекст**

Маркетплейс добавляет функцию подбора товаров через диалог с ИИ. Пользователь пишет потребность свободным текстом, ИИ ищет подходящие позиции в каталоге и возвращает готовую подборку. Время работы ИИ нестабильно и может достигать десятков секунд, поэтому выбрана асинхронная схема:

1. Front отправляет сообщение в Chat Service по REST и сразу получает 202 Accepted.  
2. Chat Service публикует запрос в Kafka-топик ai.recommend.requests.  
3. AI Service подбирает товары вместе с актуальными ценами и публикует результат в ai.recommend.responses.  
4. Chat Service сохраняет результат и доставляет его клиенту по WebSocket.

Название и изображение товара Front получает из каталога по product\_id и sku. Данные чата (сессии, запросы, рекомендованные позиции) хранятся 24 часа, затем удаляются.

### **1.4 Границы проекта**

| Область | Состав |
| :---- | :---- |
| В границах | Взаимодействие Front (клиент), Chat Service и AI Service: REST API, WebSocket-канал, Kafka-топики, хранение данных Chat Service |
| Вне границ | Внутренняя логика поиска, ранжирования и получения цен в AI Service; контракт API каталога; корзина и оплата; дизайн UI; учёт конверсии (покупка, добавление в корзину); история сессии; сервис авторизации; восстановление чата на другом устройстве или после очистки данных браузера  |
| Связанные артефакты | Sequence-диаграмма (Front → Chat Service → Kafka → AI Service), ERD (ai\_chat\_sessions, ai\_recommendation\_requests, ai\_recommended\_items) |

### **1.5 Описание изменений**

| Компонент | Изменение |
| :---- | :---- |
| Chat Service | Новый REST-метод POST /chat/message; REST-метод статуса запроса GET /chat/requests/{request\_id}; WebSocket-эндпоинт; producer в ai.recommend.requests; consumer из ai.recommend.responses; воркер повторной отправки неопубликованных запросов в Kafka; контроль таймаутов; очистка данных по сроку хранения |
| AI Service | Consumer из ai.recommend.requests; producer в ai.recommend.responses; получение актуальных цен и валюты из каталога; идемпотентная обработка по request\_id |
| Kafka | Два новых топика, DLQ-топики |
| БД Chat Service | Три таблицы: ai\_chat\_sessions, ai\_recommendation\_requests, ai\_recommended\_items |
| Front | Генерация request\_id (UUID), отправка сообщения, подключение к WebSocket, обработка событий completed и failed, сохранение в localStorage идентификаторов отправленных запросов и восстановление состояния чата после перезагрузки страницы или переподключения, получение названия и изображения товара из каталога  |

### **1.6 Аутентификация и доступ**

* REST: JWT в заголовке Authorization: Bearer \<JWT\>, выдаёт существующий сервис авторизации. Chat Service проверяет подпись и срок действия, user\_id берёт только из токена. Без валидного токена возвращается 401.  
* WebSocket: токен передаётся в заголовке Authorization: Bearer \<JWT\> HTTP-запроса установки соединения. Невалидный токен: рукопожатие отклоняется с 401. Токен истёк во время соединения: сервер закрывает его с кодом 4401, клиент обновляет токен и переподключается.  
* Примечание: нативный WebSocket API браузера не позволяет задавать произвольные заголовки. Если Front — веб\-приложение, нужно согласовать способ, поддерживаемый браузером (например, Sec-WebSocket-Protocol или одноразовый ticket).  
* Доступ к данным: пользователь работает только со своими сессиями и запросами. Чужой session\_id или request\_id даёт 403, по WebSocket приходят события только по сессиям пользователя.  
* Аутентификация сервисов при работе с Kafka выполняется по принятым в компании стандартам и вне границ ТЗ.

## **2\. Процессы и сценарии использования**

### **2.1 Общая схема взаимодействия**

1. Front генерирует request\_id и вызывает POST /chat/message, получает 202 Accepted.  
2. Chat Service записывает запрос (PENDING, published\_at пусто) в БД и после коммита публикует событие в ai.recommend.requests. При подтверждении брокера заполняет published\_at. Если публикация не подтверждена, воркер отправит запрос повторно (п. 3.6).  
3. AI Service читает запрос, подбирает товары из каталога вместе с актуальными ценами и публикует результат в ai.recommend.responses.  
4. Chat Service читает результат, сохраняет его в БД и переводит запрос в COMPLETED (или FAILED).  
5. Chat Service отправляет событие recommendation.completed (или recommendation.failed) по WebSocket.  
6. Если ответ не пришёл за 60 с, Chat Service переводит запрос в TIMEOUT и отправляет recommendation.failed.

### **2.2 Диаграмма последовательности**

![][image1]

### **2.3 Жизненный цикл запроса**

| Статус | Описание | Кто и когда устанавливает |
| :---- | :---- | :---- |
| PENDING | Запрос принят и ожидает ответа ИИ | Chat Service при создании запроса |
| COMPLETED | Ответ ИИ получен, позиции сохранены | Chat Service по сообщению SUCCESS |
| FAILED | ИИ вернул ошибку | Chat Service по сообщению ERROR |
| TIMEOUT | Ответ ИИ не получен в срок | Chat Service по истечении таймаута |

Переходы: PENDING → COMPLETED, FAILED или TIMEOUT. Статусы COMPLETED, FAILED и TIMEOUT финальные.

### **2.4 Сценарии использования**

**UC-1. Подбор товаров по текстовому запросу (основной сценарий)**

* Актор: авторизованный пользователь маркетплейса.  
* Предусловие: пользователь авторизован, открыт AI-чат, установлено WebSocket-соединение.  
* Основной поток:  
  1.  Пользователь вводит «нужен набор для кемпинга на двоих» и отправляет.  
  2. Front генерирует request\_id и вызывает POST /chat/message.  
  3. Chat Service создаёт сессию (если нужно) и запрос PENDING, публикует событие в ai.recommend.requests, возвращает 202.  
  4. Получив 202, Front сохраняет запись о запросе в localStorage (см. UC-4) и показывает индикатор «ИИ подбирает товары»   
  5. Front показывает индикатор «ИИ подбирает товары».  
  6. AI Service читает запрос, подбирает товары из каталога вместе с актуальными ценами и публикует результат в ai.recommend.responses.  
  7. Chat Service сохраняет ответ и рекомендованные товары, переводит запрос в COMPLETED, отправляет событие recommendation.completed по WebSocket.  
  8. Front отображает ответ и список товаров, название и изображение каждого получает из каталога по product\_id и sku.  
* Постусловие: в БД сохранены сессия, запрос, ответ ИИ и рекомендованные позиции.

**UC-2. Ошибка или недоступность ИИ (альтернативный)**

1. На шаге 5 AI Service возвращает ошибку или не отвечает за отведённое время (по умолчанию 60 с).  
2. Chat Service переводит запрос в FAILED или TIMEOUT, заполняет error\_message, отправляет событие recommendation.failed.  
3. Front показывает сообщение об ошибке и предлагает повторить (с новым request\_id).

**UC-3. Потеря WebSocket-соединения (альтернативный)**

1. Front читает записи из localStorage и удаляет устаревшие (старше 24 часов).   
2. Для каждой записи Front вызывает GET /chat/requests/{request\_id} (п. 3.2).  
3. Front отрисовывает чат: для каждой записи показывает user\_message, а результат по статусу:  
   COMPLETED: ответ ИИ и список товаров (название и изображение берутся из каталога по product\_id и sku); 

    FAILED или TIMEOUT: сообщение об ошибке и предложение повторить (с новым request\_id); 

   PENDING: индикатор «ИИ подбирает товары».

4. Front устанавливает WebSocket-соединение. Если есть запросы в статусе PENDING, результат придёт событием recommendation.completed или recommendation.failed ).

**UC-4. Повторная отправка запроса клиентом (альтернативный)**

1. Front не получил 202 (обрыв сети, таймаут) и повторяет POST /chat/message с тем же request\_id.  
2. Chat Service находит существующий запрос и возвращает 202 с текущим статусом, новый запрос не создаётся.

### **2.5 Фоновые процессы Chat Service**

| Процесс | Периодичность | Что делает |
| :---- | :---- | :---- |
| Повторная отправка запросов | 15 с | Находит запросы в статусе PENDING, у которых не заполнено published\_at (публикация в Kafka не подтверждена брокером), старше 15 с и с retry\_count \< 3\. Публикует их в ai.recommend.requests, при подтверждении брокера заполняет published\_at, при каждой попытке увеличивает retry\_count. Запросы с заполненным published\_at не повторяются, даже если ответ ИИ ещё не получен  |
| Контроль таймаутов | 10 с | Переводит запросы в PENDING старше 60 с в TIMEOUT, отправляет recommendation.failed |
| Очистка данных | 1 час | Удаляет запросы с позициями и сессии без запросов по сроку хранения 24 часа |

## **3\. Операции**

### **3.1 POST /chat/message**

**Назначение:** принять сообщение пользователя, при необходимости создать чат, поставить запрос в очередь к ИИ.

**Заголовки запроса**

| Заголовок | Обязательный | Описание |
| :---- | :---- | :---- |
| Authorization | да | Bearer \<JWT\>, из него берётся user\_id (п. 1.6) |
| Content-Type | да | application/json |
| X-Correlation-Id | нет | Если не передан, генерируется Chat Service |

**Тело запроса**

| Поле | Тип | Обязательное | Описание и валидация |
| :---- | :---- | :---- | :---- |
| request\_id | UUID | да | Идентификатор запроса, генерируется клиентом. Повтор с тем же значением возвращает существующий запрос |
| session\_id | UUID | нет | Идентификатор существующей сессии. Если не передан, создаётся новая |
| message | string | да | Текст запроса, после trim от 1 до 2000 символов (лимит ориентировочный) |

Пример:

{  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": null,  
  "message": "нужен набор для кемпинга на двоих"  
}

**Ответ 202 Accepted**

| Поле | Тип | Описание |
| :---- | :---- | :---- |
| request\_id | UUID | Идентификатор запроса, по нему клиент сопоставляет WebSocket-событие |
| session\_id | UUID | Идентификатор сессии (новой или существующей) |
| status | string | PENDING; при повторном запросе — текущий статус |
| created\_at | timestamp | Время создания запроса (ISO 8601, UTC) |

Пример:

{  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  
  "status": "PENDING",  
  "created\_at": "2026-10-12T10:15:30Z"  
}  
**Коды ошибок**

| Код | Причина |
| :---- | :---- |
| 400 | Невалидное тело: пустое сообщение, превышена длина, невалидный UUID |
| 401 | Нет или невалиден токен |
| 403 | Сессия или request\_id принадлежат другому пользователю |
| 404 | Сессия с переданным session\_id не найдена |
| 409 | request\_id уже использован с другим текстом или сессией |
| 429 | Превышен лимит частоты запросов |
| 500 | Внутренняя ошибка |
| 503 | Невозможно принять запрос (БД недоступна) |

### **3.2 GET /chat/requests/{request\_id}**

**Назначение:** получить статус и результат запроса по request\_id, если клиент пропустил WebSocket-событие (обрыв соединения), перезагрузил страницу или переподключился (UC-3). Метод вызывается Front и после каждого (пере)подключения WebSocket для запросов без финального статуса. 

**Заголовки:** Authorization: Bearer \<JWT\> (обязательный), X-Correlation-Id (необязательный). **Параметр пути:** request\_id (UUID).

**Ответ 200 OK**

| Поле | Тип | Описание |
| :---- | :---- | :---- |
| request\_id | UUID | Идентификатор запроса |
| session\_id | UUID | Идентификатор сессии |
| status | string | PENDING, COMPLETED, FAILED или TIMEOUT |
| message | string | Текст ответа ИИ (ai\_raw\_response), только при COMPLETED |
| items | array | Позиции (product\_id, sku, item\_price, currency), только при COMPLETED |
| error\_message | string | Описание ошибки при FAILED и TIMEOUT |
| created\_at | timestamp | Время создания запроса (UTC) |
| completed\_at | timestamp | Время завершения, заполнено для финальных статусов |

Пример (COMPLETED):

{  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  
  "status": "COMPLETED",  
  "message": "Для кемпинга на двоих я подобрал палатку, спальники и горелку.",  
  "items": \[  
    { "product\_id": 1001, "sku": "TENT-2P-GRN", "item\_price": 8990.00, "currency": "RUB" },  
    { "product\_id": 1002, "sku": "SLB-WARM-L", "item\_price": 3490.00, "currency": "RUB" }  
  \],  
  "created\_at": "2026-10-12T10:15:30Z",  
  "completed\_at": "2026-10-12T10:15:48Z"  
}  
**Коды ошибок:** 401 (нет или невалиден токен), 403 (запрос принадлежит другому пользователю), 404 (запрос не найден или удалён по истечении 24 часов), 429, 500\.

### **3.3 WebSocket: уведомление о готовности ответа**

**Назначение:** доставить клиенту результат подбора, когда ответ от ИИ получен и сохранён.

**Подключение**

* Эндпоинт: wss://\<host\>/chat/ws.  
* Аутентификация при установке соединения: п. 1.6.  
* Одно соединение получает события по всем сессиям пользователя.  
* Heartbeat: сервер шлёт ping каждые 30 с, при отсутствии ответа в течение 10 с соединение закрывается. Клиент переподключается с экспоненциальной задержкой (1, 2, 4, ..., макс. 30 с).

**События сервер → клиент**

Общий формат: type, event\_id (UUID), request\_id, session\_id, timestamp, payload.

| type | Когда отправляется |
| :---- | :---- |
| recommendation.completed | Ответ ИИ получен, запрос переведён в COMPLETED |
| recommendation.failed | ИИ вернул ошибку (FAILED) или не ответил вовремя (TIMEOUT) |

Пример recommendation.completed:

{  
  "type": "recommendation.completed",  
  "event\_id": "b1a2c3d4-0000-4000-8000-000000000001",  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  
  "timestamp": "2026-10-12T10:15:48Z",  
  "payload": {  
    "message": "Для кемпинга на двоих я подобрал палатку, спальники и горелку.",  
    "items": \[  
      { "product\_id": 1001, "sku": "TENT-2P-GRN", "item\_price": 8990.00, "currency": "RUB" },  
      { "product\_id": 1002, "sku": "SLB-WARM-L", "item\_price": 3490.00, "currency": "RUB" }  
    \]  
  }  
}  
Поля payload для recommendation.failed: status (FAILED или TIMEOUT), error\_code, message (текст для пользователя, без технических деталей). Пример:

{  
  "type": "recommendation.failed",  
  "event\_id": "b1a2c3d4-0000-4000-8000-000000000002",  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  
  "timestamp": "2026-10-12T10:16:30Z",  
  "payload": {  
    "status": "TIMEOUT",  
    "error\_code": "TIMEOUT",  
    "message": "Не удалось подобрать товары вовремя. Попробуйте ещё раз."  
  }  
}  
**Правила**

* Событие отправляется только после успешного сохранения результата в БД.  
* Доставка по принципу at-least-once, клиент дедуплицирует по event\_id.  
* Если у пользователя нет активного соединения, событие не буферизуется, результат доступен через GET /chat/requests/{request\_id}. Клиент обязан запросить статус после каждого (пере)подключения (UC-3), иначе событие, пришедшее до установки соединения, будет потеряно. 

### **3.4 Kafka-топик ai.recommend.requests (очередь запросов к ИИ)**

Общие правила для обоих топиков: формат сообщений JSON (UTF-8), поле schema\_version для версионирования, гарантия доставки at-least-once, обработка идемпотентна по request\_id. В заголовках сообщений: correlation\_id, trace\_id.

| Параметр | Значение |
| :---- | :---- |
| Producer | Chat Service (после коммита в БД, повторная отправка — воркером) |
| Consumer | AI Service, consumer group ai-service-recommend |
| Ключ сообщения | session\_id (порядок сообщений внутри сессии) |
| Партиции | 6 (уточнить по нагрузке) |
| Replication factor | 3, min.insync.replicas \= 2, acks \= all |
| Retention | 24 часа |
| DLQ | ai.recommend.requests.dlq после 3 неудачных попыток обработки |

Схема сообщения:

| Поле | Тип | Описание |
| :---- | :---- | :---- |
| schema\_version | string | Версия схемы, например 1.0 |
| event\_id | UUID | Уникальный идентификатор события |
| request\_id | UUID | Идентификатор запроса (ai\_recommendation\_requests.request\_id) |
| session\_id | UUID | Идентификатор сессии |
| user\_id | bigint | Идентификатор пользователя |
| raw\_user\_input | string | Исходный текст запроса пользователя |
| created\_at | timestamp | Время создания запроса (UTC) |

Пример:

{  
  "schema\_version": "1.0",  
  "event\_id": "c5d6e7f8-1111-4222-8333-444455556666",  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  
  "user\_id": 123456,  
  "raw\_user\_input": "нужен набор для кемпинга на двоих",  
  "created\_at": "2026-10-12T10:15:30Z"  
}

### **3.5 Kafka-топик ai.recommend.responses (очередь готовых ответов от ИИ)**

| Параметр | Значение |
| :---- | :---- |
| Producer | AI Service |
| Consumer | Chat Service, consumer group chat-service-recommend-responses |
| Ключ сообщения | request\_id |
| Партиции | 6 (уточнить по нагрузке) |
| Replication factor | 3, min.insync.replicas \= 2, acks \= all |
| Retention | 24 часа |
| DLQ | ai.recommend.responses.dlq после 3 неудачных попыток обработки |

Схема сообщения:

| Поле | Тип | Обязательное | Описание |
| :---- | :---- | :---- | :---- |
| schema\_version | string | да | Версия схемы |
| event\_id | UUID | да | Уникальный идентификатор события |
| request\_id | UUID | да | Идентификатор исходного запроса |
| session\_id | UUID | да | Идентификатор сессии |
| status | string | да | SUCCESS или ERROR |
| ai\_raw\_response | string | при SUCCESS | Текстовый ответ ИИ пользователю |
| items | array | при SUCCESS | Рекомендованные позиции, может быть пустым |
| items\[\].product\_id | bigint | да | Идентификатор товара в каталоге |
| items\[\].sku | string(50) | да | Артикул |
| items\[\].item\_price | number(12,2) | да | Актуальная цена из каталога на момент подбора |
| items\[\].currency | string(3) | да | Код валюты ISO 4217, например RUB |
| error.code | string | при ERROR | Код ошибки |
| error.message | string | при ERROR | Описание ошибки (пишется в error\_message) |
| completed\_at | timestamp | да | Время завершения обработки (UTC) |

Пример успешного ответа:

{  
  "schema\_version": "1.0",  
  "event\_id": "d7e8f9a0-2222-4333-8444-555566667777",  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  
  "status": "SUCCESS",  
  "ai\_raw\_response": "Для кемпинга на двоих я подобрал палатку, спальники и горелку.",  
  "items": \[  
    { "product\_id": 1001, "sku": "TENT-2P-GRN", "item\_price": 8990.00, "currency": "RUB" },  
    { "product\_id": 1002, "sku": "SLB-WARM-L", "item\_price": 3490.00, "currency": "RUB" }  
  \],  
  "completed\_at": "2026-10-12T10:15:47Z"  
}  
Пример ответа с ошибкой:

{  
  "schema\_version": "1.0",  
  "event\_id": "e9f0a1b2-3333-4444-8555-666677778888",  
  "request\_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",  
  "session\_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",  
  "status": "ERROR",  
  "error": { "code": "CATALOG\_UNAVAILABLE", "message": "Каталог недоступен" },  
  "completed\_at": "2026-10-12T10:15:40Z"  
}

### **3.6 Повторная отправка запросов и таймауты (внутренняя операция Chat Service)**

Назначение повторной отправки: повторять только те запросы, которые не были доставлены в Kafka. Запросы, которые уже приняты брокером и находятся в обработке у ИИ, повторно не отправляются.

1\. При приёме запроса Chat Service сохраняет его в БД со статусом PENDING, published\_at \= NULL, retry\_count \= 0 и фиксирует транзакцию.

2\. После коммита Chat Service публикует сообщение в ai.recommend.requests и дожидается подтверждения брокера (acks \= all). При успешном подтверждении заполняет published\_at текущим временем (UTC). При ошибке или отсутствии подтверждения published\_at остаётся пустым, запрос будет подхвачен воркером.

3\. Воркер повторной отправки раз в 15 с выбирает запросы со статусом PENDING, у которых published\_at IS NULL, возраст (now − created\_at) больше 15 с и retry\_count \< 3\. Вместе с запросом из ai\_chat\_sessions берётся user\_id. Воркер публикует сообщение в ai.recommend.requests, увеличивает retry\_count на 1 (первая публикация не считается повторной) и при подтверждении брокера заполняет published\_at.

4\. Если retry\_count достиг 3, а published\_at по-прежнему пустое, запрос больше не повторяется и ожидает таймаута (п. 5).

5\. Воркер контроля таймаутов раз в 10 с переводит запросы в PENDING старше 60 с (значение настраивается) в TIMEOUT. В error\_message записывается причина: «Ответ AI Service не получен за 60 с», если published\_at заполнено, или «Запрос не опубликован в Kafka», если published\_at пусто. Затем отправляется recommendation.failed.

6\. Дубликаты возможны (например, сервис упал после публикации, но до записи published\_at). AI Service дедуплицирует сообщения по request\_id (NFR-5). Повторный ответ по запросу, уже получившему финальный статус, игнорируется и логируется.

7\. Поздний ответ ИИ по запросу, уже получившему финальный статус (в том числе TIMEOUT), игнорируется и логируется.

**Нефункциональные требования**

* NFR-1. Время ответа POST /chat/message: p95 ≤ 300 мс (значение ориентировочное).  
* NFR-2. Время от отправки до уведомления по WebSocket: p95 ≤ 30 с (значение ориентировочное).  
* NFR-3. Ограничение частоты: не более 10 сообщений в минуту на пользователя, при превышении 429 (значение ориентировочное).  
* NFR-4. Идемпотентность: request\_id служит ключом идемпотентности. Повторный запрос с тем же request\_id не создаёт новый запрос, а возвращает существующий.  
* NFR-5. Доставка запроса в Kafka: Chat Service публикует сообщение после коммита в БД и фиксирует подтверждение брокера в поле published\_at. Если подтверждения нет, воркер повторно отправляет запросы в статусе PENDING с пустым published\_at старше 15 с (до 3 попыток). Запросы, уже опубликованные в Kafka, воркером не повторяются. AI Service дедуплицирует сообщения по request\_id.   
* NFR-6. Сквозная трассировка: X-Correlation-Id передаётся в заголовках Kafka-сообщений и логах всех сервисов.  
* NFR-7. Chat Service масштабируется горизонтально, WebSocket-уведомление доходит до инстанса, который держит соединение пользователя (например, через Redis Pub/Sub).  
* NFR-8. Хранение данных: записи чата (сессии, запросы, рекомендованные позиции) хранятся 24 часа, затем удаляются целиком. Сообщения в Kafka также хранятся не дольше 24 часов.  
* NFR-9. Анонимные пользователи не поддерживаются: user\_id обязателен.

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnAAAAFcCAMAAAB/S3NBAAADAFBMVEUAAAAHBwgLCwsPDxAREREXFxgYGBgeHiAnJycmJigvLy8tLTA3Nzc0NDg7Oz8+PkJHR0dGRkpMTExLS1BTU1NUVFlfX19fX2VjY2NiYmhvb29paXB3d3dxcXh4eH99fYSDg4eFhYmMjIyMjJWPj5iSkpKWlp+ampqbm6Wenqijo6OgoKmmprCvr6+oqLKtrbixsbGysr22tsG+vr66usa+vsnFxcXAwMzHx9PPz8/KytfNzdnX19fS0t/W1uPa2trZ2efe3uzj4+Ph4e/i4vDv7+/v7/D19fX///8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADz5Ep5AAAosUlEQVR4Xu2dj1/TVvfHT5sa21qgVotFEEFEEcUx3JyPjz+2PdPHP9nv5sPLOXXTISCT3yKKApW62pbQYGjS771J2qYhgbKml5Se92srybm3aTz99P7KPfd67gGCsMNrNiBILUHBIUxBwSFMQcEhTEHBIUxBwSFMQcEhTPGZDRriSg58JwNmM4JUibXgpNU2nrx8uWBOQJDqsKpSpSlvJw/Ad/ZOS+Y0BKkKK8G97ikcnVkw2hGkaiwEJ/q5wiEXEI0pCFItFoJbiZWOW1dKxwhSPRaC2+JLx3yudIwg1WMhOI/tCYJUi4XgfIauqVRsziGIE1gI7mS8dLx2snSMINVjIbhAx1zhcB4fNiDOYiE44Nt1xcmnDP0HBHEAy0dbLYGlGA9SHB9tIU5jKTjg21ZyGy1YnyKOYy04CPTAXPEBF4I4hlUbDkFqBgoOYQoKDmEKCg5hiqnTsLxRPMwUh3+PtBeNCFIdJsEZpDV3rnSMIA6BVSrCFBQcwhQUHMIUFBzCFBQcwhQUHMIUFBzCFBQcwhQUHMIUFBzCFBQcwhQUHMIUFBzCFFvB5QWzBUGqx1Zw0zg7CakBNlFbMNcdcNuEuE8Js6U6vOcaZ6WexCezZW94z5st/xSP9eZuy3Qm5jIfNdv3kbkzZku1vO4zWw4qs1WHfNoVTHvGukoV1Jm/7RmzHUGqxFJw0gftbw+uKY04jFVRmX/drx+9vtA4zRxnmVXMFmsE7sjZhlqDz0JwM2cKeoN+6U3DtHMcZeas2WLN/NcAX4Jm60HGokrtMK7x24EL5/8DKl04NEt/73xDNVy2C245ZDwLNRvPXIG8tJolndYls50iry2tmm0q82/NllryvsJI3lU133uzuXbk/6avabO5nHmzwUm2CS4pl59Hk+Xn+86HxyuLT/OQKoVsQ8lHj6dWFyz9JZRlrzW5ChdyVNR8DFeKj4/R11mzuZyaPmMyteHk2f5IuQUiU+crrCCY8IdyS+/I/OFp7YbEitjUyy98FKAvQG6/u1NLG5ECpyMwG1jhApcAFqXzIR+srnw50VNIqilCk/pKq4rk0UK3a22ta8Wv355OSqtMmtScLJCmLk5cLp6lpryBc4HVZamP+GNpK8F9S4yz54Hezeqi71gPSQp8XczuDKYSbslizcsLlpXXPvFFOKF/g4mO8EIcUsHTwhiE/dEo/VVwK3+rBfRiy5ngOKk63p48Hk9B/r0XMgJMBs9IxaSaklYFRFu/C+mC3lITHFdeKheECaFdqjjneO9r/VQsT+Xxs6f53OJ0y5nxFMDb1Mnj5E+eCCJDvDR5JiYBSQqmjO93AFMJ111+quKxMu4XG+DXjw51QCYR6yVF8G/pY/EOzbg15m36FuR330Nsk5xynfBxORyXaKp86jzESkm1ZOO4fjD7/qeC7e/g9t/yprbE6GHDsvG1Ze04Bx+69JNsrhXa4MXpHvi4+DVwQx6YDIPmSPndqRh5JUmxMYeLOIthEVfjhbx+RHoz/i0Q365vQbalkHwjmfw8eXE9N5vP074fyRN9Dx+P0i92/QTNUEyqKXobZCp+EfRKfz7xRf3qxLmjnS9FaLpI0/NaPnYtlo0LcHytSz8JBqYiUS6Xnc1LW8RVpChOyNzHNpq0nqPOWidJeac3WysJTqRfQ/Gbcyvh8Js27QuijYG8+OS7pvVnclGFnmPH4EFvAPRnzeRf15NZTN2kx8F4mLwWkxjwoI8OaEaj8OXRt735v6neEqmvAMh/X9L74Wk5+vlzU2paL2q5f4P0ewSOapUDFULLy6NaDRqEDHFWQE9ylFIbbvoZwZBSYKK4bJcrOJ8bA3m10JU+BGswQ/4KokQ1Jydk0oDiD0dTpb5/bFVt3gH/Lg0JKEuqMaE3tL8nTj8dgaxmSf1FmgAw/fzpiG5gy2pvd3d3a0L/dQqrkM8fii6mIK3X6bHkqjZdgz/+jvjyMEkCp6t7Q5V6t3RopNYNnj3SdGX2f3mfWvATfN2L79vXScn2u/ItLTP+UrzKNYCLD32K93stS+uk/rAk9KevKVqWVGOGRkeHQjB+qN/zp/4D0X7eG6cCnnVDPmZ8pCVW+wddQ7nZeeloN0yO+OCUZmmdy+q+6p98mDsabR7xKac1g2OUpieNDtHXmeDW+iAsvg9dCMwE5dW2bhj2huCb0hv2jdL0pNRhw3r+UpbWlAVkUQ7QsS0p6wtsaxp9EWnWUlLNpidpt/ryK2l066ZW6fd1zP39L3jedCzdC2KuCdbT9Lufp+Ud4U2tZx7aTE9KBQ4DdUjTNlfpTia+0kdsHGvrGy50nxQCN1Jrt0jjo/MWDMcyn250/7F+OeQvjdy4A6PCgC8bY+V0/5RbdQ5T91onOUxeGwzhr0m/Xel+5j1VbLi08g9b+x/4lFPGLTAKDVDmaI60doiaZp1UFQbB3Vb7S+SDxNxJgIhA+3jBmnfoDiJHRPo8nvQNgL8JPaeAJyUZLcSuEvd+D3ArG9S+SJ+k/hWPFN958DEITvvRkR+jTIXnVdQWR4WzbBAjLRnjBJBtpUSx3Ahk1SNhP3qs+8W2Z6mEI7QbJxQGWJE9c6TCp5Et2pRqoZFKOCvBee68efDz8UH9bED4+X5ZMrIbHq6y5/GBLTUf10izXG2CaIQjrnPC4nGH95rLL9SqlwrS+8pGE4QEfWyozRmpJW+iVfouf8hs+adYlXCEkOv0Bt3vzJYqibeaLY7Bm+Z42RGiJVylc5mq4My7Kp9QOTf8a1PCuRJnH3koTRXOkvwnyFuVDVzJX4K5N8UZ/TVkocKfgDUO+specG6Lg64Y99443pltlYogtQEFhzAFBYcwxb4NhyA1AEs4hCkoOIQpKDiEKfaCc3aYlSHuvXG8s50EhyA1AAWHMGW3R35LWe9RGsmzlAz2cADxNaU9qsZJAWhzLRa1MOnn7YanbdoM68VYlVMUkAMIZ7uQ2TH1ddrbn3sYmZ253HZs9PXp++1nYkcAjh9/q1zSosuXP5wCcT0ACy0yR0MxJJEH+XUTKTmXP5zJKymOEwRVd0k/UaGYyvnJC51wmJTpJIn85wBd2iqddVCb2o27Ebyz3Us4oMHRQvw8UcvFX5Owbc7Bx+PiCynYFYPkUranB8ZT4PvX7NpaaJCkwMuUT4hlcr5bID7js5diI5lg7gZ5WbgG45ubvn9xiZf+UPyO9MzrOxMzXxk5kFQguEWIAK1VA95PreORcPm8guSFF0RceQDhxvJkD/TzMDLbH6DVbPICCLc9M95BeAgw1toff3UieccDAnkh2bWMk53nRtVEeFBYowY52OzaaYg/mO8NaYEgPmVokF8ZMaYm+YDQD0DE0gYxRQI+MaNoE/VJCrR5oPUcXeFDzMTSASWjzIrAkxeSnV8gGYXNXugBIIlp/V3IQcdecPrQTOynuz3gp92EJekMxAa/F7Qug8YEjYVTOUQuJYv3j/eps1i1FH1eshc4CLW03G251xdYOERe7ufF+z0kIw9bQOMQ1cTChaqG3ZjSXsE720lwRnpXliD5uoOfIW24TUMDU1BaIDyRV1VD4WBDXKFragEoRgXx4QmABIgARzfUyc6ko0AyErM8D+oVHN5lBnErFbThCO3y6zmlox+Sv/igy7A9zYcIXUHjF/6ibuM7nvKxFCTvN18vX2RyaPxhzh994vVJQ6NeX6+H73ikkIxD4//rzXiHxn/hfW7a9AapHfbTk0yzjkV14CK/yRlDPh6fpZ1LWSoNasiFZSripm6nQBf0EMHvIS80u3Y9gOXZH8llN6pZdFRebjcujsFuuvRewTvbSXAsEN5udjgxHpIc59s7LVZkQVzH/grOMR4qXuWKc/0OpGbYP2moK5rXFPj4QW6urA+E7B/2nYZhs8HtKNLiu+LaOz8aUxD3YC+4DmbtSAcYBp/yNalSh3WhufPXwq5pvlfY3Zm94OqJJPjbsNNQFxwIwSUnsMNQLxwEwcnCTSzd6gV7wTGr1auGK9/Byr2416Xs7gzHERCmoOAQpqDgEKbYC47dFKmGwb0uZXdn9oJDkBrQCIIbWTZb9og8vC14yA7cSGUXGkFwoWpjEOXKZfTcbEDKOQjjcPZowYifDqvTj8eT/BUtIFH8PZjtj6Vf+JRvQo8UnzCwkPPd9DxqTm0OJDJw0wMPvb7sT/CoWcj29CRHgvrs0Ec5vrtdS0qO8OH4nV8HonD/Dif+6dNiIOHaVPZxaHA4mPNdM95GAfe6lN2d2QvuIKAFI+onmdvcUy0g8QWdZAwvWy9NjN+A256Zybvw8G03CLdnJ7sG6RHJFs97tNDHV519o/oVbnOgJxmMAGO3IP4qpIY/9idvgPCDZ//2a3M9B7tKVYMRCycxTg9IFOnORB4xewF6BQk80KoANG8AtHlalXP0SCTZAhkt9FHInocu/Qp0kryaZDSCmCEmRQ9/JPCzIo2bRCw52CXc8+ZDvuLey3TzMNL9b5a1TTJEoh8/DSRTf3VeRY1q1I5EOkpwTgt9lLweMARcaEmy0SjCNLkqf2Xxt4vq+ip85je+17F9DQ4a9oJjN0WqZkjJq+Kiuknd0qb6rwn7rngSEQjnPfmtSGh88EWz5SyTCMlWPA6+ujQORXfoSZqxeZnGmkXC33kgIYWuwv2THp586lVIPrcUnHtdyu7O7AV3ANCDEQlJbSN1PSBx6Bc+91V0cPSB/0pZ/iIKyfCTfjzwYjiWpFPujEmase+5OtGTXs7fr4Y/wtn7zX3jPknf6hnZhn0QDTvRO0hxxq/+txCMWEQLSJQlupYTaPvjWpHfMOxgLpTFMOpJ+a3hOxyI6nU0kxb+CHowpBXudSm7OzvYnYaCBEp4VPFwAVURtnoj2Qwz7MpjZvUkTVLadTRToPBhAc1aZCJddtrY2FepzDRfp/C2dYOZy5B+B6db3OxSdndmLzjEOVouL05CW7fZ3IgcVMG5MGxrcRFjFw+u4Nz23S6uYgmnYi84dh2Xg44qNqo297qU3Z3ZCw5xivWLlsPLjQkKrvZcNhsamQM+Doe4DXvBMavVGwf3upTdndkLDkFqAAoOYQoKDmGKveDYhSo2DO51Kbs7sxccgtQAFNx+wTJa1kUxPSi4/YJltOxbs2H/sH/SwG5opmHQXTqe8m/e1KNlHzUn+Rva5p7aPqB6tGxzytedyOR+8DxqFoSBRNJ30yNqQbWPFF95tGxzMd42OZULx+8ND0Sl4TucFkA7kuF916bWlneIlqWw+7LtBYfUin4eYMoQLatv7qntA6pHywq3PT93DdL9toUbM1q0bGGXz9ue8mjZUrztq1tQipZVA2g3tGjZQLdromWxSmUPn1iYMUbL6pt7qvuAFqJl2zygnFO3yWsDPVq2sMunxxQtW4y3FbJgiJZVA2h5ZVafDO+WaFn7Ei5Zvj0bUj26S+//NwqPCzY/ja8O8XC3YJA52hfQYmTBGC1bykX3A1WKQRc0DlJN0uJtwSvTI46YCPdgYf6/VGh8HzmynRXP7su2L+E+mQ1ItRRcuqFrY0kb/tI391T3AQ2EpmCmWdWKGeMun4HQNLwuXkBPCpBWHTGG4jBPLjpBK1BJhKOgFpSkh0GPbGD3ZduXcEit6HjqV1S/69Gy+uae2j6g9tGyZbt8aoGxpQuoSQPDoEXLnvykB9AKo16l10P3E3VLtOwBj0t1FwWXGvf71FA399TtO0XLGiIWzdGy9FQ4sjV8r7jVqBYtW4iRNUXLPgrTOLIC7L5s+xLuuNmAVEvBpZxZb3roq2a31ZseVKtjjpbVbBxtjGmX10zFjzJ95i2A9FikIDp2X7Z9G45ZM7JxYOBS7qrZYk/LzabJp9rTCgZ3pmNfwiGO48LYRfi1dMikGYKCYwiTb3QPyEur8B3HdgdGe8GxG5ppGNzkUiq2f6nBi2yxF9wnF3nngOAmlz4J70/wor3gkAPNLbOBEfa9VASpAfaCYzc00zCgS3cSnIvaGwcFdOlOgkOQGoCCQ5hiL7jCst2IY6BLdxIcuylSDQO6dCfBIUgNQMEhTLF/0oCDRo5TNy59lPNBdqB92AfK9/DoFozH74H4wpftjy11Sk+/JzmmPsGmH76VXgINaiTZvw3AQ/+m7wY8Cm0KXee0aEcY9m36y59p2AsOB40cp35cerYTHgK0XoIJepb+RGSiRTGC9DRGTf0Aj28A/HkLaFDj2c4/Fvvhe4ARkpT5Pvn8nB7tSK7zh+GygFUqYo94AaCXhvf/1eMFUY1ihK0/wn2GHFmSQwDIprPkx5RYmFGIMQaRUCHaMZuOF3dz1EDBIXaIHIBfBFjwdtOQCHXq+7w/aVjRhAaf0SjFtWk+BDC7dYhWmIcB+MLeoGvTr00z5u0Fh4NGjlNnLo2Mk4q0BWDjOj0Jj0Begv6rPxjmCEeIzF40A3RdC02ANNh3kv4LF2E5FQmNA90btOvajfLYix3acG6avHVAqDeXZh6o5deAejI0+jD3VZRUqwOGLRoHSY4rADMzocvAP/QrtH13/KHUC3q048yMN1jMrIJhggxxp0t38JlUFkJWjDg0YMyhRTk+6oocoflsoh3tSzgEKdfMtuBGKM9RiH7UKlFrve0guLoZNKofGsKlu0wlthdcnbU36gGXu7SisK3t9e7esBcc0mhUoqWKRLkT9sMiCFID7AVXZ4NG9QC6dCfB4eQtx0GX7iQ4BKkBKDiEKfaCa4hBI7bUjUvTE4/MJqewHxZx+aBRPVInLpWfbcJ5s9Ep7AWHNCbpd6Qzfb7dbHYKFBxiRH6m+JQa6m2HNhwOGjmOe11KHyDQBw3DkJOy4LV6TK9RydOIHbEXHA4aOU4duJS7NRgCZaJ2Pw17wSGNSeTqbZ8yXjPFoeAQM7SUU6O1aoF9p6FuBo3qh7pxaWQPi+/vEfsSrk4GjeoJdOlOgkOQGoCCQ5hiL7ia9VMaF3TpToKrg0GjegNdulMvFWkkqo5VqBQUHAL6Y62qH1tVgr3gygeN/ghepgs0jQOkwvQ8BWEY/COYSzbTIZufI6HNSCek3/EffiJnnZvx/k71bSOfbCP7CYvGrZ4kurVsxTxu198700oHG6ThO8U94N1L3YzD1RB7wVkOGg0CjA/SgxEf/XMZ0r/Ts/+Qb/txJ8xehWDeQ89eLamCkz91qW8TxRBPV+MhL2n5qAdkryLzIG9JHEcM9JMSoZJgSGoySnI2e8hxMhTQlg2g/+t5+RZZ2dKXEuj10kz7sm3UnrF0aYNhL7jK0L5pKhZF7YWd+hyhZz66UBjA21BfPEa0KAWzP4ovJOiPiS992UuxJ6GM1HVudm0tNPjQ6zsTg+Rsjhacywkq4yfNST4qPvNKl2Lwv2AuPMA9HoiSMkx6puYdyeV8zZsrazfUj/i9vTs5EjQt0YO4looFlyK16abZSNdm0skfB7p2nUegP+N8vFU1rrbCByK4EJWRtoLi2C2Iv4pB5mb6+an+AKkWW/vhwQnPq1swSvJsaqvXZW5zMNbaH391wtPZpyaojKl5N5J3Ie9JFqpUwqvOvmIexOXYC860uWeYtuHKLIQ0X9hxczR7Qy3n1JfR7G11nZ1XUm4qOXcuSfWWFEiJ5Elm/lAXOznNRSKL/dRMDKHPVMqXSEepp0e9WBdH7F41oY8kaJ9AdKiaIl//zPcaJwgmN/vUN7sfN+2Xul/YC66SxcyKTadR4d9Ea6RVpRZwwr+1dZ3i4RyE4+dCtLkVUhthIbimpoikNIvSg9AQbYqJpAYWCtdSCYGWIHMgtIBXVhdbVE0Qvbvwl1FwAUXmyt/sVipx6UHHfuB3L4xnhkAGiMkwR5r44+REXZdTuXL58pVsmp/IQ54PT+TzEh/Ok0Y/kaK0vNkOGwA0LQEBUui9Jm+ITxWuSLPThGk1IRSHeVLIqiZJhKMkfaP44YHQtPpmpB6wL+EKyMvtu444xGltew8u/OLlaQMuDvQM4Dgp6Ljwu8u5X/jcT0Ojv3i/ig6RQ38Ujj/O9fKQvN98XfmF90VhYBhipMuRWqPVrMrQ+C9eknGNJAD0PR8++YmaSF5hlJd64eyr+8VRlIEXw/TNSD1gvwImbXDIS6ubA2rV5yyPurRhuoqgYyeVssNqji7AnW04tj6zL+GIc9JjfglqoDcI20dpbIezqfYf0pfWYpFYB7hSb4yxFxwsripKVl9R2GEumw07YVe+3aQvNmJE3Ap31mwpkJhVcgAdm38fm3vZPfdy6++XW8eGt47N7XJuYdrt3MK02zk1fY6+/qtnvjwL6KM0ZQ/NkB0p+IqNz3Zswy29JiXgLmu2ugu27ZG9gm24naqkTwCdNyK5HPb/HAPnw+3YhiPwg7D06pIrf5dIfbKz4Aid7cstds12BNkr9oIrTN7i9jBkhuwIzofbqQ2HFanjoEt3EhyC1AAUHMIUe8HhcIjjoEt3EhwOGjkOunQnwSFIDUDB7Tcjy2bLHpGHDbvQux57weGgkeOYXSolBYCvT9JDOa9Okk7m6YmoNvbotHliJsdCkh4lpUIrME3/aAaQRZn+UbMm9CRIpAvnxKJdJ09ziTRB/xT9mDX2A784aOQ4JpeOp/ybN7nf1fizJ83J8+3iMz57KSa+kIJdsfQLn/JN6ElzytedyOR+8DxpFoSBRNJ30yNq0ZJPFF/2LiRHeDUwXQuuDJUCKa89CWX4z+cg/RK060g9PfB7MHcDtE8ZydBj9tgLDqk1/TzAVGECaSk0UounfNl6aWL8Bgi3PT93DdKpQ8KNmcmuwYdvu7VoSQ/c9iyXhUiSKzwtBVKS85vc/VOBl7dAu87KZI/wAw1u0j4leUcLdGKNfZWK1Bo+sTCTKZzEOBAzsXRAyQhEgx4xewF6BQnaPKCcAxpu1Aat5Kh5A0iutELe54GYJGTPQ1fpCloSr4gkkZ6HVsQskOuQd3tiisTPilD4FIUe7wP2JZw7J2/VNSaX3v9vtBTq66dTm0M83C0YZI426g5pZYJSOPKSo2Iur8SBUpxZQa6gJ92Dhfn/0vcoanmiXccLcqAPFs7on3IPyPE+FHL2JRwOGjmO2aUbarQtwNKc+ocPT9AYShoNKQVCUzDTrC2fYkKLltTQQyT1C5QFUkJcgs12GoA506znJv2Go4VPEenxPmBfwiG1puOpX1H9n9TX0Bgaf5jzR4dGf+EvRgdHH/ivGHMX0aIl9RMtRLJ0gWIgpQeOP/b28jD4AIrXEcZ90k/6pzzx0mP22E8xnztnttQBbKdL7xWzS2XJHL0mBGgNqdn15aEsyG8YFu/Jl9WLWpLo99BQTIEeS1C6jgh+mlv9FP2Ytc/sSzjzoBFSNapL5aXVti7tq+bMeqPLrkDBbqs38BgXiypvh2lJ9P3hgHY142X0zyvm2QfsBYd9Bsehob7vkpHrZntN2FMoJjvsBYc4Dy3dBvaha+gi7HupSG3Qlmp0EYWmG5Mm3A6Cw8lbjpPkui+u/zZhNrsAdl+2veDMg0ZI1RCXtly+2fR00XXTO9h92faCQ2oDLeWemI2NA3Ya2NPi0g4kE+xLOByHcxz3upTdndkLDsfhHMe9LmV3Z/aCQ5AagIJDmGIvOHZDMw2De13K7s7sBcduaKZhcK9L2d2ZveAOHg0WkOdOGkRwNB6vFJBXDKcT1Ug7oPFyskyOaTwezSAVNsYpC8hLFALyyBFNUoPt6HvJFdWU7QF56X0MyHMn9gO/7IZmas/45qbvX6WAvEI4nfh7MNtfCMhTfMLAQs530/OkObU5kMjATQ9oWx3SgLyentKehU9yfHc7Scr+BCMZ3ncNxjK8cBeKgX16QJ7vmr4jonpM3+hel7K7M3vBsRuaqT39PIzMGgLytHA6z4sfoRSQd9szM3kXHr7tBuH2rBaQp291SAPyJnsMAXm3OXWDiHh+I3nHowXkJcVAMbBPD8jL6wF5G+oxxb0uZXdnjVGl8gszNLBOoxhOJ9IqsBiQ54FWBWgYHrR59IA8sRSQpxgD8ugEbZIUyPDKrB6QF1kpBfbpAXkePSBPPS58eMNjX8IdIMRf78FUsedfDKfTWmSFgDxQf300DK8YkMeVAvLAGJBHoEmgB+R56BQ3bymwTw/Im/9Rj9qjx7ahI42GfQnHbmim5tASaUU90uPptHA6urVhfqeAPG2rQ43CnoV6QB5NAkNA3nJ72XUKAXlq1F4xIM+9LmV3Z/Yl3AHa3JPveKTEUvRIj6fTI+3o1oZf7RCQB9pWhxr6noWFr0ahOyQKo15FC8iT7oLxOjQgr1ffEVE7prjXpezurEHCBEVzjJIWTidLaqhchQF5atRdkfyGGmxHL7DUKRwxXUcPwsvTergYkOdel7K7M/sSrv4xBOSZ9aaH0+lxerZ6Kw/IK9ObISAP9IA843X0D/TQdt+2D29k7AXHbmimRkwkIxeLe6TXlEp3+HSvS9ndmb3gmNXqNcGVAXnudSm7O7PfvrIuKW7F+C7TsZiz3PVyl40xzecWpt3OLUzaefm9Nib2nYa6xNhpIFXqaTZVKlI5B3gc7vLNpkl3BeS516Xs7sxecOymSNUKrvu6uwLy3OtSdndm32k4EFw2G5B9xr6EQ5AaYC84dkMzDYN7XcruzuwFx25opmFwr0vZ3Zm94BCkBqDgEKbYC47d0EzD4F6Xsrsze8GZh2bGx+dcNYpah5hd6h7Y3Zm94MrJP/Y3ffrVbEWQPVLhwG/+/853Q4+YjEx92vSR9+R8mzcDIP7py15KfoJNP3yLk76QSrAXXNnQTEad8xV4H+mHx52d8OhsJ90Hb+wWxF/9BPB4P/ZBrEPYjXbtFXZ3Zi+4sqEZWZvNWlyBO5sWIyBm0hBw3aLcLobdaNdeYXdn9oIrIwACvadg4XwtlRsEEaYBmg25EGQ3KhXcwPN7kBwvhqZ0dcLT65Hwd55iGB2CVIK94Mo392yXH3hzHcXTmRnvNVDD7PyVTuhHzPulugh2d2YvOFOoYmdUjz6iHYRbmo2/o4bKqSZkd9hFf+4Vdne26zhc+pF+ELAY+PCEjMsfIMiu2JdwhPRY5HSLXpohiBPYC+744ircdlugXX3DbrRrr7C7M/swwXfLHf6kVbDbzucWpt3OLUy7nVuY1PNMIUzQlSF5Fo0Sl8DuznYIEyQl3Hf11kSzW1sEcQv2VSp0d4Ncj6JD3Ix9L5VOkeK6r190VaBdfcNu1tleYXdn9oLTp0hhL9U52M062yvs7sxecAhSA1BwCFPsBcduaKZhcK9L2d2ZveCYPV1rHNzrUnZ3Zi84BKkBKDiEKfaCYzc00zC416Xs7sxecOyGZhoG97qU3Z3ZCw5BaoD9w3t2s44rJLlmtmxno3BwxGi1xOPpreHkqzmraDZl+897nQv1MHhYvZvvLO7MiMfj2MYh9g/v3aa3ZcV2JtU/Q5p3zIvbkDvsNxsxIgEvFffVrB1V+66wD171lJQtpgmGFLex0Wa2VAlvVQg5xHxleoN3PPBnzEbnEar1XYX/nAoolXDTtOF4t5RSYMJfu4LgoCIdMlusyVL3++y3+jqAGKpUVWwzwa31QVh8H7oQmAnKq23d8MmbgW9KuZAKeFthqbWs7jL4tpF+0YbGYj6fB0i9/jsGC/Pnck/k1OvPkdkE8MG2agvkRkPbcHx3ZN37leY/CBhKuP8DCN0A31WAxa5YbHgW+CFIrUZ5f3spD1IJ2p6X6m6XyaOFrvDaWteKv7OUibCuLZ0RMu6RedAxCO62unVymHQfcidJJ1Wgy4YEneueNBDpJvq68BX531vo7Kcm2rmUai8hhNU/oXRjCk6L3PHSTeHJjzMb3KQFfg17cv+QDwuHlex/PM+bLhiM89qWy/BrPrB1Qj8uY4y/aDbVjo0T2t+p+IU2SKyITb38fILbpA4W5452vhShSb0bUVs7IxgvvLHmrE7+RF6fXzXbyxj72mxxEKtxOE/rXGwpc3FaO/MLsreGI6R7ZW2m7SJk6UbLRgoFcfNlzvq5YC19aMdfiYEoKdqCx5bHvotsyOoiLH8eOwXBVnnZnJcR8ebE7mvB1NRXVoKDr549UM4UNuLrHfufYvs4gj1vgv36smF/eFq79fJj4fMY9JECRD7GaSPWI1LgdARmAytc4BJpk0rn533dqytfTvQUkljwsY9+uaS8jf6WPva3Wpwl2no1S3pf9jmUkv2rJcGlpryBc4HVZamP+GNpK8F9S4yz59XqYnXRd6yHJAWcVl9JcEPq6zX6wl2ne7jTw8ukpr1ezOICvgjdenGbOLOxEIilgq1LY9+FP0dp+xO4lSNh+nexpSk9/iOkP3bJCx3h/Ps2yPAw2d7+uZjEgtCbMGmciW8/5yGr6Su1Sj95ej2XL/6emfLe1zqdK3zl8ni/nMotLp5qGh8Kw9sjJ+VUGPJeutwpLC5ckjZg+lRTOqW1Mx3D5hlayEWVaBkiHNaPjnWcP7ECHbn3ufXVY4EO7VnSd/Dm+SSkFpSUQhvoRzu7u8fh5RHaqkt9cyF2oZTEgGvXXv0G4pOO65dBX/89fGmettyuXr+8PwvCL37FHR8tnh365DnXtBBVUk1TxFVXOrvHZXipNoBTC9/EOi+kSJIyVczuDDaCcy3B4vN5eud5GN84danwdRI8x3qvrsoyeLwcLULIj/mULCXVNrysNvuKSSzghw5lD8GaNFO0tH6YhUNrYLCwZfZZJrOuH3PfilNPBeA83pYTWlUnr0laA1hW6wuZJHF6/8cxLNtwpJ+1rAB3cvcpF8zhwx/PGjoM4sZ3TdSBZUOncghKY/d8ZNmrDlw3xWnBZkyqHXm9guCHnl3pXnx3qvAdkwb5uNy/+F45VTQA/XoZIUebSbX6odC9569Iz5egubTMZGRVHZkgvoLPxFkhvRftKNaCkz6SGkqKb9Z+HsOeOf/n2DfyWqsuOlJ+NNHSQhC5Q+RLlpMRLh3iIUpaHoVmeWwxqmbm37W0JKKHjUm14ohIezVfkf/5m9BzCnhST1GdXyWV6vcAt7JB7emp/hT1C6sf9movua8va33ambDels8fii6Gwmkxplpik5tal4I//q5TTtIkiGtJjmE1H056XSgF5s6657nyXPH5ZOqwYbEfKWts1cqiHKC3LGV96tqcZXwRadZS0mvd846Tn6/sWaqYUJ88vKnlxDyV2R6zRSUVoA1iKdu0zVW6k4mv9DFp64LpH2B1odfF2+t57cIyjj4MKcGX/SQ43T/lVp3Dan/DMslZKtVP4Iv2t9L8TqM50tohapp1UlVYdBo2/EXBc0HRmIJUCFfhE0Fvjr5alC8HFwvBrRhq7daV0jFSMadWzRZrTrynr2U9iIOOheBkQznKqz9BZI/wXZUVcaHOLEhzztdbLsaiDVfWotiv5kWdw32ocNaD4A2dN9sONBaCM055lhqqfeEgLEb76hKLKvWkIaRs7WTpeJ+psMhALHDRlGLLcbiFwoyy12dc1L7YLbZyj9Q2LtVlVOk7B+NSrQQHwntNcfJWA01FRZhgUaWSzlPPB9I7zX1YRL0hDmPRaQDarQeYO9dlNiNItViWcAhSK1BwCFNQcAhTUHAIU1BwCFNMvdTl4op+kJkrHB3BtR4QpzAJziCtOccGlxGkCFapCFNQcAhTUHAIU1BwCFNQcAhTUHAIU1BwCFNQcAhTUHAIU1BwCFNQcAhTUHAIU1BwCFNsBZcXzBYEqR5bwU3j7CSkBtgJbq47VJyAiSCOYR2XCsukfDu3zO++awmC7AnrEk5QZ/62Z8x2BKkSS8FJH7S/PZUtq4cgFWMluHxxee/XLlrnCTkQWAhuZqu4dHn/1n5tmYIcUCwEp+1apcF3LJROEKRqtgtuuWyNrlCz8QxBqmSb4JKmnZ+ilvvdIsg/wyQ4eSqi7sZjIDLFbPMx5OBjEtyScSN5nQtLZguC/FNMTxq6y09VPFZGBPlHbGvDIUgtQcEhTEHBIUxBwSFMQcEhTEHBIUxBwSFM+X+JuUXM7uNbtQAAAABJRU5ErkJggg==>