"""create data table

Revision ID: 904a4f629019
Revises: 
Create Date: 2025-07-08 19:42:53.017092

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision: str = '904a4f629019'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'location_data',
        sa.Column('longitude', sa.Numeric(precision=7, scale=4), nullable=False),
        sa.Column('latitude', sa.Numeric(precision=7, scale=4), nullable=False),
        sa.Column('date', sa.Date, nullable=False),
        sa.Column('timezone', sa.String(50), nullable=True),
        sa.Column('data', JSONB, nullable=True),
    )
    op.create_primary_key("pk_location_data", "location_data", ["longitude", "latitude", "date"])
    op.create_index('idx_location_data_date', 'location_data', ['date'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('idx_location_data_date', table_name='location_data', if_exists=True)
    op.drop_table('location_data', if_exists=True)
    op.drop_index('pk_location_data', table_name='location_data', if_exists=True)
