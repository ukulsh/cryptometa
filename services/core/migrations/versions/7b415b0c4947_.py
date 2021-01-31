"""empty message

Revision ID: 7b415b0c4947
Revises: 25fd1f7fde7d
Create Date: 2020-04-26 21:39:02.850078

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7b415b0c4947'
down_revision = '25fd1f7fde7d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint('order_id_unique', 'shipments', ['order_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('order_id_unique', 'shipments', type_='unique')
    # ### end Alembic commands ###