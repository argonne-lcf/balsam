"""denormalize parents

Revision ID: 0901ab2d43b6
Revises: 700eda0f93f8
Create Date: 2021-09-28 12:50:09.863376

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg


# revision identifiers, used by Alembic.
revision = "0901ab2d43b6"
down_revision = "700eda0f93f8"
branch_labels = None
depends_on = None


def upgrade():
    # ALTER TABLE jobs ADD COLUMN parent_ids integer[] NOT NULL DEFAULT '{}';
    op.add_column(
        "jobs", sa.Column("parent_ids", pg.ARRAY(sa.Integer, dimensions=1), server_default="{}", nullable=False)
    )
    op.execute(
        "UPDATE jobs SET parent_ids = ARRAY(SELECT job_deps.parent_id FROM job_deps WHERE job_deps.child_id = jobs.id);"
    )
    op.drop_table("job_deps")


def downgrade():
    pass
