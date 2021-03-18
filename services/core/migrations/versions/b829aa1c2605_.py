"""empty message

Revision ID: b829aa1c2605
Revises: 488dd4113873
Create Date: 2021-02-27 21:47:12.923432

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b829aa1c2605'
down_revision = '488dd4113873'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('thirdwatch_data', sa.Column('channel_order_id', sa.String(), nullable=True))
    op.add_column('thirdwatch_data', sa.Column('client_prefix', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('thirdwatch_data', 'client_prefix')
    op.drop_column('thirdwatch_data', 'channel_order_id')
    # ### end Alembic commands ###