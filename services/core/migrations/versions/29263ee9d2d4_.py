"""empty message

Revision ID: 29263ee9d2d4
Revises: 131f9c272fc7
Create Date: 2019-12-11 00:40:56.784391

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '29263ee9d2d4'
down_revision = '131f9c272fc7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('orders', sa.Column('status_type', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('orders', 'status_type')
    # ### end Alembic commands ###
