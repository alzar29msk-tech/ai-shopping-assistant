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

