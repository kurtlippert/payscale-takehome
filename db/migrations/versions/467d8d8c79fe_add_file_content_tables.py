"""Add file content tables

Revision ID: 467d8d8c79fe
Revises: f60a94779933
Create Date: 2020-08-17 11:21:24.852210

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '467d8d8c79fe'
down_revision = 'f60a94779933'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('file_content',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('upload_file_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_utc', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('modified_utc', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('number_of_columns', sa.INTEGER(), nullable=False),
        sa.Column('record_count', sa.INTEGER(), nullable=False),
        sa.Column('category', sa.String, nullable=True),
        sa.Column('content_headers', postgresql.ARRAY(sa.String()), nullable=True),
        sa.ForeignKeyConstraint(['upload_file_id'], ['upload_file.id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('data_frame')
