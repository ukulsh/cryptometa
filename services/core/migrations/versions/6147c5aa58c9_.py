"""empty message

Revision ID: 6147c5aa58c9
Revises: 53affd6fdb43
Create Date: 2020-11-09 19:03:32.795476

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6147c5aa58c9'
down_revision = '53affd6fdb43'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('client_mapping', sa.Column('thirdwatch_activate_time', sa.DateTime(), nullable=True))
    op.add_column('client_mapping', sa.Column('thirdwatch_cod_only', sa.BOOLEAN(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('client_mapping', 'thirdwatch_cod_only')
    op.drop_column('client_mapping', 'thirdwatch_activate_time')
    # ### end Alembic commands ###