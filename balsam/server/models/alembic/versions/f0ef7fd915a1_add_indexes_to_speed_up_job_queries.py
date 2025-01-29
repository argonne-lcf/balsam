"""add indexes to speed up job queries

Revision ID: f0ef7fd915a1
Revises: 8c75d069a1ab
Create Date: 2022-12-12 16:37:07.418906

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "f0ef7fd915a1"
down_revision = "8c75d069a1ab"
branch_labels = None
depends_on = None


def upgrade():
    # create index jobs_app_id_fkey on jobs (app_id);
    op.create_index("jobs_app_id_fkey", "jobs", [text("app_id")])


def downgrade():
    op.drop_index("jobs_app_id_fkey")
