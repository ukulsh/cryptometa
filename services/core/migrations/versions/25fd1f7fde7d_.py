"""empty message

Revision ID: 25fd1f7fde7d
Revises: 8a5e2578555b
Create Date: 2020-04-17 17:44:03.992686

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '25fd1f7fde7d'
down_revision = '8a5e2578555b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('cost_to_clients', sa.Column('management_fee', sa.FLOAT(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('cost_to_clients', 'management_fee')
    # ### end Alembic commands ###
