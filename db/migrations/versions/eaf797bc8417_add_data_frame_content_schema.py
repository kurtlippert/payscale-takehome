"""Add data_frame_content schema

Revision ID: eaf797bc8417
Revises: 467d8d8c79fe
Create Date: 2020-08-17 14:39:40.405073

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eaf797bc8417'
down_revision = '467d8d8c79fe'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE SCHEMA data_frame_content")


def downgrade():
    op.execute("DROP SCHEMA data_frame_content")
