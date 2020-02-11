"""empty message

Revision ID: bea0c8af00c5
Revises: 0f97745358c6
Create Date: 2020-01-21 02:32:37.064439

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bea0c8af00c5'
down_revision = '0f97745358c6'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('orders', sa.Column('pickup_data_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'orders', 'client_pickups', ['pickup_data_id'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'orders', type_='foreignkey')
    op.drop_column('orders', 'pickup_data_id')
    # ### end Alembic commands ###
