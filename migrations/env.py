from alembic import context
from sqlalchemy import create_engine, pool

from journeysignal.adapters.config.settings import Settings
from journeysignal.adapters.persistence.models import Base

config = context.config
url = config.attributes.get("database_url")
if url is None:
    settings = Settings()
    url = (settings.migration_database_url or settings.database_url).get_secret_value()

if config.attributes.get("connection") is not None:
    context.configure(connection=config.attributes["connection"], target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()
elif context.is_offline_mode():
    context.configure(url=url, target_metadata=Base.metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    engine = create_engine(url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=Base.metadata)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()
