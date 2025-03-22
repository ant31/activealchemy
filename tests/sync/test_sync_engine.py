"""
Unit tests for the synchronous ActiveEngine
"""
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from activealchemy.sync import ActiveEngine


def test_engine_creation(db_config):
    """Test creating a sync engine"""
    engine = ActiveEngine(db_config)
    assert engine is not None
    assert isinstance(engine, ActiveEngine)
    engine.dispose_engines()


def test_engine_session(sync_engine):
    """Test getting a session from the engine"""
    engine_obj, session_factory = sync_engine.session()
    
    assert engine_obj is not None
    assert isinstance(engine_obj, Engine)
    assert session_factory is not None
    assert isinstance(session_factory, sessionmaker)
    
    # Create a session and test it
    session = session_factory()
    assert session is not None
    assert isinstance(session, Session)
    session.close()


def test_engine_connection(sync_session):
    """Test connecting to the database"""
    # Test the connection by running a simple query
    result = sync_session.execute(text("SELECT 1 as value"))
    row = result.fetchone()
    assert row is not None
    assert row[0] == 1


def test_engine_multiple_schemas(db_config):
    """Test using multiple schemas with the same engine"""
    engine = ActiveEngine(db_config)
    
    # Get engines and sessions for two different schemas
    engine1, session_factory1 = engine.session("public")
    engine2, session_factory2 = engine.session("other_schema")
    
    # They should be different objects
    assert engine1 != engine2
    assert session_factory1 != session_factory2
    
    # Clean up
    engine.dispose_engines()


def test_engine_args(db_config):
    """Test engine creation with custom arguments"""
    # Test with custom engine arguments
    engine = ActiveEngine(db_config, echo=True)
    assert engine is not None
    
    # Check that the arguments were passed to the SQLAlchemy engine
    sa_engine, _ = engine.session()
    assert sa_engine.echo is True
    
    # Clean up
    engine.dispose_engines()
