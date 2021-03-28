"""empty message

Revision ID: e685fe5e17b9
Revises: 96dfc9bfb1f9
Create Date: 2021-03-28 11:29:14.066621

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e685fe5e17b9'
down_revision = '96dfc9bfb1f9'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('orders_invoice', sa.Column('qr_url', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('orders_invoice', 'qr_url')
    # ### end Alembic commands ###
