# This example uses SQLAlchemy to connect to
# the TEIID service from SERPRO.
import sqlalchemy
from sqlalchemy.dialects import registry

registry.register("postgresql.teiid", "sqlalchemy_teiid", "TeiidDialect")

engine = sqlalchemy.create_engine(
    "postgresql+teiid://{usr}:{pwd}@daas.serpro.gov.br:35432/NovoSCDP"
)

engine.connect()
