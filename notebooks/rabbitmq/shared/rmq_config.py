import os

# Host is read from environment variable so Docker can inject it.
# Falls back to localhost for running outside Docker.
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")

# Exchanges (analogous to Kafka topics).
# We use fanout exchanges so every bound queue gets every message.
EXCHANGES = {
    "stock_ticks":   "stock-ticks",
    "news_posts":    "news-posts",
    "trade_signals": "trade-signals",
}