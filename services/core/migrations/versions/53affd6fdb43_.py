"""empty message

Revision ID: 53affd6fdb43
Revises: fa2e7bb687c1
Create Date: 2020-11-07 17:15:05.721032

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '53affd6fdb43'
down_revision = 'fa2e7bb687c1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('client_mapping', sa.Column('thirdwatch', sa.BOOLEAN(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('client_mapping', 'thirdwatch')
    # ### end Alembic commands ###
