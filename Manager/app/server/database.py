from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
SQLALCHEMY_DATABASE_URL = "postgresql://test:12345@localhost:5432/test"
# SQLALCHEMY_DATABASE_URL = "postgresql://oseyvxxg:WidizAZOBw5KKHIj1eq73gk5_sWUZhfH@ziggy.db.elephantsql.com/oseyvxxg"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

from .controllers.SqlDM import *

def get_db():
    db = SessionLocal()
    databaseManager = SqlDM(db)
    try:
        return databaseManager
    finally:
        db.close()