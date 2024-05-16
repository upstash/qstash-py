import os

import dotenv

QSTASH_TOKEN = os.environ.get(
    "QSTASH_TOKEN", dotenv.dotenv_values().get("QSTASH_TOKEN")
)

QSTASH_CURRENT_SIGNING_KEY = os.environ.get(
    "QSTASH_CURRENT_SIGNING_KEY",
    dotenv.dotenv_values().get("QSTASH_CURRENT_SIGNING_KEY"),
)

QSTASH_NEXT_SIGNING_KEY = os.environ.get(
    "QSTASH_NEXT_SIGNING_KEY",
    dotenv.dotenv_values().get("QSTASH_NEXT_SIGNING_KEY"),
)
