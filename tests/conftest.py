import pytest
from src.common.spark_session_manager import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """Fixture pour fournir une session Spark unique pour toute les tests"""
    session = get_spark_session("Pytest-Session")
    yield session
    session.stop()
