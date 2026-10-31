# Архитектурные схемы сервиса (C4 Model)

## 1. Контекстная схема (System Context)
Схема отражает место сервиса торговой стратегии в окружении платформы, пользователей и внешних систем, а также направление и назначение каждого взаимодействия.

```mermaid
flowchart TB
    Student["Разработчик / Оператор (Студент)"]
    Teacher["Владелец платформы (Преподаватель)"]
    
    subgraph Infrastructure ["Внешняя инфраструктура"]
        Service["Сервис торговой стратегии (momentum-strategy)"]
        Kafka[("Брокер сообщений Apache Kafka")]
        Platform["Преподавательская платформа"]
        MarketProvider["Внешний поставщик рыночных данных"]
    end

    Student -- "Настраивает .env, запускает Docker, проверяет GET /health и логи" --> Service
    Teacher -- "Управляет прогонами (replay), темами и портфелями через UI" --> Platform
    MarketProvider -- "Поставляет исторические и текущие свечи" --> Platform

    Service -- "1. Публикует запрос регистрации (account.registration.request)" --> Kafka
    Service -- "2. Публикует пульс раз в 5 сек (account.service.heartbeat)" --> Kafka
    Service -- "3. Публикует торговые сигналы (strategy.recommendation.created)" --> Kafka

    Kafka -- "4. Доставляет результат регистрации (account.registration.result)" --> Service
    Kafka -- "5. Доставляет закрытые свечи (market.candle.closed)" --> Service
    Kafka -- "6. Доставляет результаты сделок (strategy.recommendation.result)" --> Service

    Platform <-->|"Читает запросы/рекомендации, публикует свечи и расчеты портфелей"| Kafka
```

---

## 2. Крупноузловая компонентная схема (Component Diagram)
Внутренняя структура модульного монолита (DDD / Clean Architecture) с разделением зон ответственности и основных потоков данных.

```mermaid
flowchart TB
    subgraph External ["Внешняя среда"]
        KafkaBroker[("Apache Kafka")]
        Admin["Оператор / Мониторинг"]
    end

    subgraph App ["Контейнер сервиса: momentum-strategy (Python / FastAPI)"]
        
        subgraph Adapters ["Слой Adapters (Инфраструктура и ввод-вывод)"]
            HttpHealth["FastAPI Health Controller (main.py)\nЗона ответственности: HTTP GET /health"]
            KafkaWorker["KafkaAdapter (kafka_worker.py)\nЗона ответственности: SASL_SSL соединение, регистрация, heartbeat, чтение/запись топиков, ручной commit offset"]
            StateStore["StateRepository (state_store.py)\nЗона ответственности: Хранение обработанных eventId (идемпотентность) и истории цен по (runId, ticker)"]
        end

        subgraph Ports ["Слой Ports (Интерфейсы)"]
            PubPort["RecommendationPublisherPort"]
            RepoPort["StrategyStateRepositoryPort"]
        end

        subgraph Application ["Слой Application (Сценарии использования)"]
            UseCase["ProcessCandleUseCase (use_cases.py)\nЗона ответственности: Координация проверки дублей, вызова ядра, замера processingTimeMs и отправки ответа"]
        end

        subgraph Core ["Слой Core / Domain (Предметная область)"]
            DomainLogic["Strategy Logic & Entities (logic.py, entities.py)\nЗона ответственности: Сущности Candle/Recommendation и чистый расчет сигнала (HOLD / Momentum)"]
        end
    end

    Admin -- "GET /health" --> HttpHealth
    HttpHealth -- "Чтение статуса подключения" --> KafkaWorker

    KafkaBroker <-->|"SASL_SSL: События свечей, регистрации и рекомендаций"| KafkaWorker
    KafkaWorker -- "Передает десериализованный объект Candle" --> UseCase
    
    UseCase -- "Проверка дубля eventId и чтение состояния" --> RepoPort
    RepoPort -. "Реализует" .- StateStore

    UseCase -- "Расчет решения по свече" --> DomainLogic
    
    UseCase -- "Передача готового объекта Recommendation" --> PubPort
    PubPort -. "Реализует" .- KafkaWorker
```