# ERD

```mermaid
erDiagram
    ai_chat_sessions ||--|{ ai_recommendation_requests : contains
    ai_recommendation_requests ||--o{ ai_recommended_items : include
    
    ai_chat_sessions {
        session_id UUID PK
    user_id BIGINT  
    created_at TIMESTAMPZ 
    updated_at TIMESTAMPZ
    }
    ai_recommendation_requests {
        request_id UUID PK
    session_id UUID  FK
    raw_user_input TEXT  
    ai_raw_response TEXT 
    request_status enum
    error_message TEXT 
    retry_count SMALLINT
    created_at TIMESTAMPZ
    completed_at TIMESTAMPZ
    published_at TIMESTAMPTZ
    }
    ai_recommended_items {
            product_id BIGINT PK
            request_id UUID PK
             sku VARCHAR(50)  
            item_price NUMERIC(12,2)
            currency  CHAR(3) 


    }
```

Почему так:

- составной PK `(request_id, product_id)`: один товар может попадать в разные подборки;
- связь запроса с позициями `o{`: при FAILED/TIMEOUT и пустой выдаче позиций нет;
- `published_at` отличает «сообщение не дошло до Kafka» (нужен ретрай) от «ИИ ещё думает» (ретрай вреден);
- `quantity`: «набор на двоих» подразумевает, например, 2 спальника.
