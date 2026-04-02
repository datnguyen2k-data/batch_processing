from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager
from typing import Generator
from src.shared.config import GovDbConfig

Base = declarative_base()

class SqlConnector:
    """Manages SQLAlchemy Engine and Sessions for the Control Plane Database."""
    
    _engine = None
    _SessionLocal = None
    
    @classmethod
    def get_engine(cls):
        """Initialize and return the SQLAlchemy engine as a singleton."""
        if cls._engine is None:
            url = GovDbConfig.get_url()
            # Pool configuration for multiple queries, but prevent holding idle connections too long
            cls._engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)
        return cls._engine
        
    @classmethod
    def get_session_factory(cls):
        """Get the localized session factory."""
        if cls._SessionLocal is None:
            cls._SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=cls.get_engine()
            )
        return cls._SessionLocal

    @classmethod
    @contextmanager
    def session(cls) -> Generator:
        """Provide a transactional scope around a series of operations."""
        Session = cls.get_session_factory()
        session = Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
