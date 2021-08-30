"""empty message

Revision ID: a7b391ebce73
Revises: 9fd0cac0bdce
Create Date: 2021-05-24 11:59:14.300882

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a7b391ebce73'
down_revision = '9fd0cac0bdce'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('client_channel', sa.Column('script_id', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('client_channel', 'script_id')
    # ### end Alembic commands ###