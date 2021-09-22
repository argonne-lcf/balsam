"""job-fk-indexes

Revision ID: 700eda0f93f8
Revises: a5121c766e26
Create Date: 2021-09-21 13:54:50.328598

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "700eda0f93f8"
down_revision = "a5121c766e26"
branch_labels = None
depends_on = None

# CREATE INDEX log_events_job_id_fkey ON log_events (job_id);
# CREATE INDEX transfer_items_job_id_fkey ON transfer_items (job_id);


def upgrade():
    op.create_index("ix_log_events_job_id", "log_events", ["job_id"])
    op.create_index("ix_transfer_items_job_id", "transfer_items", ["job_id"])


def downgrade():
    op.drop_index("ix_log_events_job_id", "log_events")
    op.drop_index("ix_transfer_items_job_id", "transfer_items")
