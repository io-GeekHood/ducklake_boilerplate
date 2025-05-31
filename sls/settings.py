from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class MinioSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MINIO_", extra="ignore", env_file=".env"
    )

    endpoint: str = Field(..., description="e.g., 'localhost:9000'")
    access_key: SecretStr = Field(...)
    secret_key: SecretStr = Field(...)
    use_ssl: bool = False
    region: str = "us-east-1"
    bucket_name: str = Field(...)


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_", extra="ignore", env_file=".env"
    )

    host: str = Field(...)
    port: int = Field(5432)
    user: str = Field(...)
    password: SecretStr = Field(...)
    dbname: str = Field(...)
