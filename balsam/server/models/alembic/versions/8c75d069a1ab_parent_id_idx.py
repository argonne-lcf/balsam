"""parent_id_idx

Revision ID: 8c75d069a1ab
Revises: 0901ab2d43b6
Create Date: 2021-10-19 10:11:40.483896

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8c75d069a1ab"
down_revision = "0901ab2d43b6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index("ix_job_parent_ids", "jobs", ["parent_ids"], postgresql_using="GIN")


def downgrade():
    op.drop_index("ix_job_parent_ids", "jobs")
