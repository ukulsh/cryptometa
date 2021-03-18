"""empty message

Revision ID: eb8b056796f6
Revises: 6993885f46f5
Create Date: 2020-12-15 21:08:19.933570

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eb8b056796f6'
down_revision = '6993885f46f5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('id_client_unique', 'orders', type_='unique')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint('id_client_unique', 'orders', ['channel_order_id', 'client_prefix'])
    # ### end Alembic commands ###