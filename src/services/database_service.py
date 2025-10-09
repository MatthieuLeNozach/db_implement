# src/services/database_service.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from core.config import Config
import logging


logger = logging.getLogger(__name__)

class DatabaseService:
    """Centralized database connection and session management."""
    
    def __init__(self, db_path: str = None):
        db_url = db_path or Config.DATABASE_URL
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    @contextmanager
    def get_session(self) -> Session:
        """Context manager for database sessions."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()