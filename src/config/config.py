from pydantic_settings import BaseSettings

class Settings(BaseSettings):

    REDIS_HOST: str | None = 'redis'
    REDIS_PORT: int | None = 6379
    REDIS_PASSWORD: str | None = None
    REDIS_TTL: int | None = 3600

    MONGODB_URL: str | None = 'mongodb://mongodb:27017'
    MONGODB_DB_NAME: str | None = 'tpp'

    REMOTE_RABBIT: bool | None = False
    RABBIT_HOST: str | None = None
    RABBIT_PORT: int | None = None
    RABBIT_VHOST: str | None = None
    RABBIT_USER: str | None = None
    RABBIT_PASSWORD: str | None = None

    class Config:
        case_sensitive = True
        env_file = '.env'

settings = Settings()