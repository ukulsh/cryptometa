"""empty message

Revision ID: 7b4a35bc2031
Revises: 986585043f1a
Create Date: 2020-01-23 11:21:42.686861

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7b4a35bc2031'
down_revision = '986585043f1a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('ndr_verification')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('ndr_verification',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('order_id', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('call_sid', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('recording_url', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('ndr_verified', sa.BOOLEAN(), autoincrement=False, nullable=True),
    sa.Column('verified_via', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('verification_link', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('verification_time', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('date_created', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['order_id'], ['orders.id'], name='ndr_verification_order_id_fkey'),
    sa.PrimaryKeyConstraint('id', name='ndr_verification_pkey')
    )
    # ### end Alembic commands ###