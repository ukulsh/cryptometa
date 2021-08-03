"""empty message

Revision ID: 577ca54f9c15
Revises: f4fd7405c310
Create Date: 2021-05-10 21:54:56.466834

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '577ca54f9c15'
down_revision = 'f4fd7405c310'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint('fld_ord_unique', 'failed_orders', ['order_id_channel_unique', 'client_channel_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('fld_ord_unique', 'failed_orders', type_='unique')
    # ### end Alembic commands ###