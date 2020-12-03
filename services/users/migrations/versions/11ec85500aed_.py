"""empty message

Revision ID: 11ec85500aed
Revises: ee33f51cd8cb
Create Date: 2020-11-11 17:18:09.548149

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '11ec85500aed'
down_revision = 'ee33f51cd8cb'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('clients', sa.Column('calling', sa.Boolean(), nullable=True))
    op.drop_column('users', 'accepted_time')
    op.drop_column('users', 'tnc_accepted')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('users', sa.Column('tnc_accepted', sa.BOOLEAN(), autoincrement=False, nullable=True))
    op.add_column('users', sa.Column('accepted_time', postgresql.TIMESTAMP(), autoincrement=False, nullable=True))
    op.drop_column('clients', 'calling')
    # ### end Alembic commands ###