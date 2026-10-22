# volatility

Сервис на Go, который слушает поток котировок криптовалют из [`crypto-streamer`](#контракт-crypto-streamer) по gRPC, считает волатильность по каждому символу на скользящем временном окне и отправляет алерт в Kafka, когда волатильность превышает порог.

> Учебный проект: сервис в активной разработке.

## Как это работает

```
crypto-streamer ──gRPC stream──► streamer.Subscribe ──quoteCh──► pipeline worker ──alertCh──► notifier.Producer ──► Kafka
                                 (горутина на символ)            (горутина на символ,          (fan-in, один
                                                                  свой Aggregator)              читатель)
```

1. **Инструменты.** `instruments.Manager` загружает список отслеживаемых символов с порогами. Сейчас источник — заглушка `cfgclient.DummyClient` (`btcusdt`, порог `0.02%`).
2. **Приём котировок.** `streamer.Client.Subscribe` открывает серверный gRPC-стрим `Quote` на каждый символ и пишет котировки в собственный канал. При обрыве — переподключение с экспоненциальным backoff (1с → 30с, jitter ±20%). Если символ не найден (`codes.NotFound`), подписка завершается.
3. **Расчёт.** Для каждого символа работает отдельная горутина со своим `aggregator.Aggregator` — без общих данных и мьютексов. Агрегатор хранит цены за последние `WINDOW_SIZE`.
4. **Проверка порога.** Раз в минуту воркер считает волатильность и сравнивает с порогом символа. Алерт уходит, если значение выше порога **и** выше предыдущего отправленного максимума; когда волатильность опускается ниже порога, максимум сбрасывается. Так сервис не шлёт один и тот же алерт каждую минуту, пока рынок «горячий».
5. **Отправка.** Все воркеры пишут в общий буферизованный `alertCh`; `notifier.Producer` читает его и синхронно отправляет сообщения в Kafka. Если буфер переполнен, алерт отбрасывается с предупреждением в логе.

### Формула волатильности

Размах цены на окне в процентах от минимума:

```
volatility = (max − min) / min × 100
```

Для расчёта нужно минимум две точки в окне, иначе результат — `0`.

### Graceful shutdown

По `SIGINT` / `SIGTERM` отменяется корневой `context` → воркеры завершаются → `Pipeline.Run` закрывает `alertCh` → продюсер дочитывает оставшиеся алерты и выходит. На дренаж отводится `SHUTDOWN_TIMEOUT`, после чего контекст продюсера отменяется принудительно.

## Формат сообщения в Kafka

- **Топик:** `KAFKA_TOPIC`
- **Ключ:** символ (например, `btcusdt`). Балансировщик `Hash`, поэтому алерты одного символа попадают в одну партицию и сохраняют порядок.
- **Значение:** JSON

```json
{
  "version": 1,
  "symbol": "btcusdt",
  "volatility": 0.03,
  "threshold": 0.02,
  "timestamp": "2026-10-22T12:00:00Z"
}
```

| Поле         | Тип     | Описание                                         |
|--------------|---------|--------------------------------------------------|
| `version`    | number  | Версия схемы сообщения                           |
| `symbol`     | string  | Тикер торговой пары                              |
| `volatility` | number  | Волатильность в процентах, округлена до 0.01     |
| `threshold`  | number  | Порог для символа, в процентах                   |
| `timestamp`  | string  | Время срабатывания, RFC 3339, UTC                |

## Конфигурация

Конфигурация задаётся только через переменные окружения. Все переменные, которые читает `config.Load`, обязательны: если какой-то нет, сервис не стартует. В `docker-compose.yml` у каждой есть значение по умолчанию, и их можно переопределить через `.env`.

| Переменная         | Пример               | Описание                                                                 |
|--------------------|----------------------|--------------------------------------------------------------------------|
| `STREAMER_ADDR`    | `cryptostream:50051` | Адрес gRPC-сервиса `crypto-streamer`                                     |
| `KAFKA_BROKERS`    | `kafka:9092`         | Брокеры Kafka через запятую                                              |
| `KAFKA_TOPIC`      | `volatility`         | Топик для алертов (создаётся автоматически, если брокер это разрешает)   |
| `WINDOW_SIZE`      | `1m`                 | Размер скользящего окна расчёта (формат `time.ParseDuration`)            |
| `SHUTDOWN_TIMEOUT` | `5s`                 | Сколько ждать отправки оставшихся алертов при остановке                  |
| `CHECK_INTERVAL`   | `1m`                 | Интервал проверки порога. *Читается, но пока не используется: интервал зашит в коде (1 минута)* |
| `LOG_LEVEL`        | `INFO`               | Уровень логирования. *Читается, но пока не применяется к логгеру*        |

Пороги и список символов в переменных окружения не задаются: их отдаёт источник инструментов (сейчас `cfgclient.DummyClient`).

## Запуск

### Требования

- Go 1.25+
- Docker и Docker Compose
- Запущенный `crypto-streamer`
- Kafka в отдельном compose-проекте, подключённая к внешней Docker-сети `kafka-net`

### Docker Compose

Kafka в этот репозиторий не входит. Сервис подключается к уже существующей сети `kafka-net`, поэтому сначала поднимите Kafka, а затем:

```bash
make run    # docker compose up -d --build
make logs   # docker compose logs -f
make stop   # docker compose down
```

### Локально

```bash
make build   # бинарник ./volatility (linux/amd64, CGO_ENABLED=0)

STREAMER_ADDR=localhost:50051 \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=volatility \
WINDOW_SIZE=1m \
SHUTDOWN_TIMEOUT=5s \
CHECK_INTERVAL=1m \
LOG_LEVEL=INFO \
./volatility
```

Или без сборки: `go run ./cmd/volatility` с теми же переменными.

## Контракт crypto-streamer

Контракт лежит в [`proto/api/v1/api.proto`](proto/api/v1/api.proto) (`cryptostream.api.v1.StreamerService`). Сервис использует метод:

```proto
rpc Quote(QuoteRequest) returns (stream QuoteResponse);
```

`QuoteResponse` содержит `symbol`, `price` (в USDT) и `trade_time` (Unix-время в секундах).

Go-стабы сгенерированы в `internal/gen/api/v1` через [buf](https://buf.build). Чтобы перегенерировать:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
make proto   # buf generate
```

## Структура проекта

```
cmd/volatility/          точка входа: конфиг, сборка зависимостей, graceful shutdown
internal/
  config/                чтение конфигурации из переменных окружения
  streamer/              gRPC-клиент crypto-streamer, подписка с переподключением
  instruments/           список символов и порогов (Manager + интерфейс источника)
  cfgclient/             источник инструментов (пока заглушка)
  aggregator/            скользящее окно цен и расчёт волатильности
  pipeline/              воркер на символ: окно → проверка порога → alertCh
  notifier/              Kafka-продюсер (segmentio/kafka-go)
  model/                 Quote, Alert
  gen/api/v1/            сгенерированный gRPC-код
proto/api/v1/            .proto-контракт
docker/volatility/       Dockerfile (multi-stage, alpine)
```
