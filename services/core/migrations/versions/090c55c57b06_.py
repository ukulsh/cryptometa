"""empty message

Revision ID: 090c55c57b06
Revises: 8666d37af88a
Create Date: 2021-06-01 15:15:18.351202

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '090c55c57b06'
down_revision = '8666d37af88a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('pincode_serviceability', sa.Column('pickup', sa.BOOLEAN(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('pincode_serviceability', 'pickup')
    # ### end Alembic commands ###