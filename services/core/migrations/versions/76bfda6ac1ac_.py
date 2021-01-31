"""empty message

Revision ID: 76bfda6ac1ac
Revises: 5e82ce459e5a
Create Date: 2019-11-22 12:17:34.389684

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '76bfda6ac1ac'
down_revision = '5e82ce459e5a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('orders_payments', sa.Column('subtotal', sa.FLOAT(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('orders_payments', 'subtotal')
    # ### end Alembic commands ###