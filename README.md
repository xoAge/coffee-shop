# Coffee Shop - микросервисы на Spring Boot и Apache Kafka

![CI](https://github.com/xoAge/coffee-shop/actions/workflows/ci.yml/badge.svg)

Учебный проект: два микросервиса, которые обмениваются событиями через Apache Kafka. 
Клиент заказывает кофе в **order-service**, бариста в **barista-service** его готовит, а статус заказа проходит путь `NEW → PREPARING → READY`. Сервисы не вызывают друг друга напрямую и не делят базу данных: они общаются только событиями.

## Что показывает проект

- **асинхронное взаимодействие** через Kafka: никакого центрального координатора;
- **database per service**: база заказов принадлежит только order-service;
- **итоговую согласованность**: статус заказа догоняет реальность через несколько секунд;
- **Transactional Outbox**: заказ и событие сохраняются одной транзакцией, 
отдельный ретранслятор публикует события в Kafka - ни одно событие не теряется, даже если Kafka недоступна;
- **идемпотентность**: статус заказа движется только вперёд, повторные и устаревшие события игнорируются;
- **масштабирование**: несколько копий бариста в одной consumer group делят партиции топика.

## Технологии

- Java 21, Maven, Spring Boot 4
- Spring Web MVC, Spring Data JPA (Hibernate), Jakarta Validation
- Spring for Apache Kafka, Apache Kafka 4
- PostgreSQL, Flyway
- Docker, Docker Compose
- JUnit 5, AssertJ, Testcontainers (PostgreSQL и Kafka), Awaitility
- GitHub Actions

## Сервисы

| Сервис | Порт | Что делает |
|---|---|---|
| `order-service` | 8081 | принимает заказы по REST, хранит их в PostgreSQL, публикует `orders.created` через outbox, обновляет статус по событиям бариста |
| `barista-service` | - | слушает `orders.created`, «готовит» заказ и публикует `PREPARING` и `READY` в `orders.status-changed` |
| `kafka` | 9092 | брокер сообщений |
| `orders-db` | 15433 | PostgreSQL для order-service |

## Топики и события

| Топик | Отправитель | Получатель | Ключ |
|---|---|---|---|
| `orders.created` | order-service | barista-service | id заказа |
| `orders.status-changed` | barista-service | order-service | id заказа |
| `orders.created.dlt` | barista-service (обработчик ошибок) | — для разбора | id заказа |

Ключ - id заказа: все события одного заказа попадают в одну партицию и обрабатываются по порядку. 
Контракт между сервисами - формат JSON.

```json
// orders.created
{ "orderId": 7, "drink": "капучино", "customerName": "Андрей" }

// orders.status-changed
{ "orderId": 7, "status": "PREPARING" }
{ "orderId": 7, "status": "READY" }
```

## Запуск

[Docker Desktop](https://www.docker.com/products/docker-desktop/).

**1.** Создать `.env` с паролем базы из образца:

```
cp .env.example .env
```

в Windows: `copy .env.example .env`, затем вписать в `.env` свой пароль.

**2.** Запустить всё:

```
docker compose up --build -d
```

**3.** Создать заказ и посмотреть, как меняется статус:

```
POST http://localhost:8081/orders
{ "drink": "капучино", "customerName": "Андрей" }

GET http://localhost:8081/orders/{id}
```

## Запуск сервисов из IDE

1. Поднять только инфраструктуру: `docker compose up -d kafka orders-db`.
2. Для `order-service` задать переменную окружения `ORDERS_DB_PASSWORD` (пароль из `.env`).
3. Запустить `OrderServiceApplication` и `BaristaServiceApplication`.

Сервисы в IDE подключаются к Kafka через `localhost:9092`, сервисы в контейнерах — через `kafka:9093`: у брокера два слушателя с разными объявленными адресами.

## Тесты

```
cd order-service && ./mvnw verify
cd barista-service && ./mvnw verify
```

Для тестов нужен запущенный Docker: Testcontainers сам поднимает PostgreSQL и Kafka.

GitHub Actions собирает и тестирует оба сервиса параллельно на каждый pull request; 
мерж в `main` возможен только при зелёных проверках.