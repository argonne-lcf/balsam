"""Serialized Apps

Revision ID: a5121c766e26
Revises: 1be17d936c7b
Create Date: 2021-09-04 14:13:15.143949

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a5121c766e26"
down_revision = "1be17d936c7b"
branch_labels = None
depends_on = None


def upgrade():
    # Update Apps
    op.add_column("apps", sa.Column("serialized_class", sa.Text(), nullable=True))
    op.add_column("apps", sa.Column("source_code", sa.Text(), nullable=True))
    op.drop_column("apps", "last_modified")

    op.drop_constraint("apps_site_id_class_path_key", "apps")
    op.alter_column("apps", "class_path", new_column_name="name")
    op.create_unique_constraint("uniq_app_name_per_site", "apps", ["site_id", "name"])

    # Update Jobs
    op.add_column("jobs", sa.Column("serialized_parameters", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("serialized_return_value", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("serialized_exception", sa.Text(), nullable=True))


def downgrade():
    pass
