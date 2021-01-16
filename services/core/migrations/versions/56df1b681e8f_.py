"""empty message

Revision ID: 56df1b681e8f
Revises: 23aa6ccc47ba
Create Date: 2020-10-02 17:43:43.065150

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '56df1b681e8f'
down_revision = '23aa6ccc47ba'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('client_mapping', sa.Column('shipping_label', sa.String(), nullable=True))
    op.add_column('client_mapping', sa.Column('verify_ndr', sa.BOOLEAN(), server_default='true', nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('client_mapping', 'verify_ndr')
    op.drop_column('client_mapping', 'shipping_label')
    # ### end Alembic commands ###