-- #####################################################################
-- 1. БАЗА: SELECT, WHERE, ORDER BY, LIMIT, GROUP BY
-- #####################################################################

-- 1.1 Последние 10 запросов пользователя 101 (JOIN + фильтр + сортировка + LIMIT)
SELECT r.request_id,
       r.raw_user_input,
       r.request_status,
       r.created_at
FROM ai_recommendation_requests r
JOIN ai_chat_sessions s ON s.session_id = r.session_id
WHERE s.user_id = 101
ORDER BY r.created_at DESC
LIMIT 10;

-- 1.2 Сколько запросов в каждом статусе
SELECT request_status,
       count(*) AS requests_cnt
FROM ai_recommendation_requests
GROUP BY request_status
ORDER BY requests_cnt DESC;


-- #####################################################################
-- 2. JOIN-ы, агрегаты, HAVING
-- #####################################################################

-- 2.1 Стоимость каждой подборки: число позиций, штук и сумма
--     Только подборки дороже 10 000. Группируем и по валюте: валюты не складывают.
SELECT r.request_id,
       r.raw_user_input,
       i.currency,
       count(*)                          AS positions,
       sum(i.quantity)                   AS units,
       sum(i.item_price * i.quantity)    AS total_price
FROM ai_recommendation_requests r
JOIN ai_recommended_items i ON i.request_id = r.request_id
GROUP BY r.request_id, r.raw_user_input, i.currency
HAVING sum(i.item_price * i.quantity) > 10000
ORDER BY total_price DESC;

-- 2.2 Успешные запросы без единой позиции (LEFT JOIN + IS NULL)
--     Полезно для поиска «ИИ ответил, но ничего не нашёл».
SELECT r.request_id,
       r.raw_user_input,
       r.ai_message
FROM ai_recommendation_requests r
LEFT JOIN ai_recommended_items i ON i.request_id = r.request_id
WHERE r.request_status = 'COMPLETED'
  AND i.request_id IS NULL;

-- 2.3 Сколько запросов и сессий у каждого пользователя; только тех, у кого запросов >= 2
SELECT s.user_id,
       count(DISTINCT s.session_id) AS sessions_cnt,
       count(r.request_id)          AS requests_cnt
FROM ai_chat_sessions s
LEFT JOIN ai_recommendation_requests r ON r.session_id = s.session_id
GROUP BY s.user_id
HAVING count(r.request_id) >= 2
ORDER BY requests_cnt DESC;

-- #####################################################################
-- 4. ОПЕРАЦИИ СЕРВИСА (то, что Chat Service реально выполняет по ТЗ)
--    Каждый блок: BEGIN ... ROLLBACK. Чтобы изменения сохранились, замените ROLLBACK на COMMIT.
-- #####################################################################

-- 4.1 GET /requests/{id}: статус + позиции одним запросом, с проверкой владельца
--     Чужой request_id вернёт 0 строк -> сервис отвечает 404.
SELECT r.request_id,
       r.session_id,
       r.request_status,
       r.ai_message,
       r.error_message,
       r.created_at,
       r.completed_at,
       COALESCE(
           jsonb_agg(
               jsonb_build_object(
                   'product_id', i.product_id,
                   'sku',        i.sku,
                   'item_price', i.item_price,
                   'currency',   i.currency,
                   'quantity',   i.quantity)
               ORDER BY i.position
           ) FILTER (WHERE i.request_id IS NOT NULL),
           '[]'::jsonb) AS items
FROM ai_recommendation_requests r
JOIN ai_chat_sessions s       ON s.session_id = r.session_id
LEFT JOIN ai_recommended_items i ON i.request_id = r.request_id
WHERE r.request_id = 'a0000000-0000-4000-8000-000000000001'
  AND s.user_id    = 101
GROUP BY r.request_id;

-- 4.2 POST /message: идемпотентная вставка запроса в существующую сессию
BEGIN;
    -- проверка владельца сессии: 0 строк -> 404/403
    SELECT 1 FROM ai_chat_sessions
    WHERE session_id = '11111111-1111-4111-8111-111111111111' AND user_id = 101;

    -- повторный POST с тем же request_id ничего не вставит (0 строк в RETURNING)
    INSERT INTO ai_recommendation_requests (request_id, session_id, raw_user_input)
    VALUES (gen_random_uuid(), '11111111-1111-4111-8111-111111111111', 'нужна палатка потеплее')
    ON CONFLICT (request_id) DO NOTHING
    RETURNING request_id, request_status, created_at;

    -- и обновляем активность сессии
    UPDATE ai_chat_sessions
    SET updated_at = now()
    WHERE session_id = '11111111-1111-4111-8111-111111111111';
ROLLBACK;

-- 4.3 Сохранение ответа ИИ (SUCCESS). Условие request_status = 'PENDING' защищает от
--     запоздалых/повторных ответов: уже закрытый запрос не перезапишется.
BEGIN;
    UPDATE ai_recommendation_requests
    SET request_status = 'COMPLETED',
        ai_message     = 'Подобрал рюкзак и чехол от дождя.',
        completed_at   = now()
    WHERE request_id = 'a0000000-0000-4000-8000-000000000006'
      AND request_status = 'PENDING'
    RETURNING request_id;      -- 1 строка -> можно писать позиции; 0 строк -> ответ игнорируем

    INSERT INTO ai_recommended_items (request_id, product_id, position, quantity, sku, item_price, currency)
    VALUES ('a0000000-0000-4000-8000-000000000006', 3001, 1, 1, 'BACKPACK-50L', 6990.00, 'RUB'),
           ('a0000000-0000-4000-8000-000000000006', 3002, 2, 1, 'RAIN-COVER-M',  590.00, 'RUB');
ROLLBACK;

-- 4.4 Воркер повторной отправки: запросы, которые НЕ дошли до Kafka (ТЗ 3.6).
--     SKIP LOCKED: несколько воркеров не берут одну и ту же строку.
BEGIN;
    SELECT r.request_id, r.session_id, s.user_id, r.raw_user_input, r.created_at
    FROM ai_recommendation_requests r
    JOIN ai_chat_sessions s ON s.session_id = r.session_id
    WHERE r.request_status = 'PENDING'
      AND r.published_at IS NULL
      AND r.created_at < now() - interval '15 seconds'
      AND r.retry_count < 3
    ORDER BY r.created_at
    LIMIT 100
    FOR UPDATE OF r SKIP LOCKED;

    -- после публикации в Kafka и подтверждения брокера:
    UPDATE ai_recommendation_requests
    SET retry_count  = retry_count + 1,
        published_at = now()
    WHERE request_id = 'a0000000-0000-4000-8000-000000000007';
ROLLBACK;

-- 4.5 Воркер таймаутов: PENDING старше 60 с -> TIMEOUT. RETURNING отдаёт список для
--     отправки recommendation.failed по WebSocket.
BEGIN;
    UPDATE ai_recommendation_requests
    SET request_status = 'TIMEOUT',
        completed_at   = now(),
        error_message  = CASE WHEN published_at IS NULL
                              THEN 'Запрос не опубликован в Kafka'
                              ELSE 'Ответ AI Service не получен за 60 с'
                         END
    WHERE request_status = 'PENDING'
      AND created_at < now() - interval '60 seconds'
    RETURNING request_id, session_id, error_message;
ROLLBACK;

-- 4.6 Очистка по сроку хранения 24 часа (ТЗ 2.5). Позиции удаляются каскадом.
BEGIN;
    WITH deleted AS (
        DELETE FROM ai_recommendation_requests
        WHERE created_at < now() - interval '24 hours'
        RETURNING request_id
    )
    SELECT count(*) AS deleted_requests FROM deleted;

    -- затем сессии, в которых не осталось запросов
    DELETE FROM ai_chat_sessions s
    WHERE NOT EXISTS (
        SELECT 1 FROM ai_recommendation_requests r WHERE r.session_id = s.session_id
    )
    RETURNING s.session_id, s.user_id;
ROLLBACK;


-- #####################################################################
-- 5. ПРОИЗВОДИТЕЛЬНОСТЬ: индексы и EXPLAIN
-- #####################################################################

-- Покажите, что воркер использует частичный индекс idx_requests_pending_created.
-- На маленьком наборе данных планировщик может выбрать Seq Scan, это нормально.
-- Чтобы увидеть индекс, наполните таблицу (например, generate_series на 100 000 строк)
-- и выполните ANALYZE ai_recommendation_requests;
EXPLAIN (ANALYZE, BUFFERS)
SELECT request_id
FROM ai_recommendation_requests
WHERE request_status = 'PENDING'
  AND created_at < now() - interval '60 seconds';
