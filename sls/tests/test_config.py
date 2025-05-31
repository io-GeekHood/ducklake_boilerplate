import pytest
import os
from pydantic import ValidationError

from sls.settings import (
    MinioSettings,
    PostgresSettings,
)


@pytest.fixture(autouse=True)
def manage_env_vars():
    """Fixture to save and restore environment variables before and after each test."""
    original_environ = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_environ)


class TestMinioSettings:
    def test_load_from_env(self, monkeypatch):
        """Test loading Minio settings from environment variables."""
        # Pydantic handles string 'true' to bool
        monkeypatch.setenv("MINIO_ENDPOINT", "minio.example.com:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "testaccesskey")
        monkeypatch.setenv("MINIO_SECRET_KEY", "testsecretkey")
        monkeypatch.setenv("MINIO_USE_SSL", "true")
        monkeypatch.setenv("MINIO_REGION", "eu-central-1")

        settings = MinioSettings()

        assert settings.endpoint == "minio.example.com:9000"
        assert settings.access_key.get_secret_value() == "testaccesskey"
        assert settings.secret_key.get_secret_value() == "testsecretkey"
        assert settings.use_ssl is True
        assert settings.region == "eu-central-1"

    def test_default_values(self, monkeypatch):
        """Test default values for Minio settings when not overridden."""
        # Required fields must still be provided
        monkeypatch.setenv("MINIO_ENDPOINT", "default.minio.com")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "defaultaccess")
        monkeypatch.setenv("MINIO_SECRET_KEY", "defaultsecret")

        settings = MinioSettings()

        assert settings.use_ssl is False  # Default from your class definition
        assert settings.region == "us-east-1"  # Default from your class definition

    def test_partial_env_override(self, monkeypatch):
        """Test overriding only some Minio settings, others use defaults."""
        monkeypatch.setenv("MINIO_ENDPOINT", "partial.minio.com")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "partialaccess")
        monkeypatch.setenv("MINIO_SECRET_KEY", "partialsecret")
        monkeypatch.setenv("MINIO_REGION", "ap-southeast-2")  # Override region

        settings = MinioSettings()

        assert settings.endpoint == "partial.minio.com"
        assert settings.access_key.get_secret_value() == "partialaccess"
        assert settings.secret_key.get_secret_value() == "partialsecret"
        assert settings.use_ssl is False  # Default
        assert settings.region == "ap-southeast-2"  # Overridden


class TestPostgresSettings:
    def test_load_from_env(self, monkeypatch):
        """Test loading Postgres settings from environment variables."""
        monkeypatch.setenv("POSTGRES_HOST", "pg.example.com")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpassword")
        monkeypatch.setenv("POSTGRES_DBNAME", "testdb")

        settings = PostgresSettings()

        assert settings.host == "pg.example.com"
        assert settings.port == 5433
        assert settings.user == "testuser"
        assert settings.password.get_secret_value() == "testpassword"
        assert settings.dbname == "testdb"

    def test_default_values(self, monkeypatch):
        """Test default values for Postgres settings when not overridden."""
        # Required fields must still be provided
        monkeypatch.setenv("POSTGRES_HOST", "default.pg.com")
        monkeypatch.setenv("POSTGRES_USER", "defaultuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "defaultpass")
        monkeypatch.setenv("POSTGRES_DBNAME", "defaultdb")

        settings = PostgresSettings()

        assert settings.port == 5432  # Default from your class definition

    def test_port_type_coercion(self, monkeypatch):
        """Test that port is correctly coerced to int if provided as string in env."""
        # Port as string
        monkeypatch.setenv("POSTGRES_PORT", "5444")
        monkeypatch.setenv("POSTGRES_HOST", "pg.example.com")
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpassword")
        monkeypatch.setenv("POSTGRES_DBNAME", "testdb")

        settings = PostgresSettings()
        assert settings.port == 5444
        assert isinstance(settings.port, int)
