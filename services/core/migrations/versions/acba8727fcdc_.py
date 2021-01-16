"""empty message

Revision ID: acba8727fcdc
Revises: 7527c46ad20c
Create Date: 2020-01-04 14:15:42.506714

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'acba8727fcdc'
down_revision = '7527c46ad20c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('order_status', sa.Column('location_city', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('order_status', 'location_city')
    # ### end Alembic commands ###