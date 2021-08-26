"""site.hostname -> site.name

Revision ID: 1be17d936c7b
Revises: f8fbad8262e3
Create Date: 2021-08-26 13:08:35.831999

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1be17d936c7b"
down_revision = "f8fbad8262e3"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("sites_hostname_path_key", "sites")
    op.alter_column("sites", "hostname", new_column_name="name")
    op.execute("UPDATE sites SET name = CONCAT(name, CONCAT(':', CAST(id as text)));")
    op.create_unique_constraint("uniq_owner_site_name", "sites", ["owner_id", "name"])


def downgrade():
    pass
