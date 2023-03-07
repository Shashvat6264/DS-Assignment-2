import os
READ_ONLY = os.getenv('READ_ONLY')

from .controllers import *

from .database import get_db, engine

from . import models

models.Base.metadata.create_all(bind=engine)

main_manager = BrokerManager(database=get_db())
main_manager.loadPersistence()

health_manager = HealthCheck(main_manager)