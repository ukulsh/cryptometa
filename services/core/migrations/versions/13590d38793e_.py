"""empty message

Revision ID: 13590d38793e
Revises: 8abc040e74f5
Create Date: 2019-11-11 21:41:48.526830

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '13590d38793e'
down_revision = '8abc040e74f5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('op_association', sa.Column('quantity', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('op_association', 'quantity')
    # ### end Alembic commands ###
