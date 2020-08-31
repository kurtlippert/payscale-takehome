"""Add UploadFile table

Revision ID: f60a94779933
Revises: 
Create Date: 2020-08-13 21:01:25.646001

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'f60a94779933'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('upload_file',
    sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('created_utc', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('modified_utc', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('file_size_bytes', sa.INTEGER(), nullable=False),
    sa.Column('mime_type', sa.String(), nullable=True),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('storage_path', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('upload_file')
