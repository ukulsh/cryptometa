"""empty message

Revision ID: 50bc5c4ba29d
Revises: 29263ee9d2d4
Create Date: 2019-12-11 19:46:11.429366

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '50bc5c4ba29d'
down_revision = '29263ee9d2d4'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('orders', sa.Column('status_detail', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('orders', 'status_detail')
    # ### end Alembic commands ###
