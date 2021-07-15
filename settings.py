from environs import Env

env = Env()
env.read_env()

ELQ_USER = env("ELQ_USER")
ELQ_PASSWORD = env("ELQ_PASSWORD")
ELQ_BASE_URL = env("ELQ_BASE_URL")

CLOUD_APP_DB_NAME = "eloqua-app-db"
DB_USER = env("DB_USER")
DB_PASSWORD = env("DB_PASSWORD")
