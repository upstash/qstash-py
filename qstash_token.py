import os
import dotenv

QSTASH_TOKEN = os.environ.get("QSTASH_TOKEN", dotenv.dotenv_values()["QSTASH_TOKEN"])
