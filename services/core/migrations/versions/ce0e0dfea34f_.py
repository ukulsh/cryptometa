"""empty message

Revision ID: ce0e0dfea34f
Revises: 7083c6a7babd
Create Date: 2020-02-06 23:39:48.163262

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ce0e0dfea34f'
down_revision = '7083c6a7babd'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('delivery_check',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('order_id', sa.Integer(), nullable=True),
    sa.Column('call_sid', sa.String(), nullable=True),
    sa.Column('recording_url', sa.String(), nullable=True),
    sa.Column('del_verified', sa.BOOLEAN(), nullable=True),
    sa.Column('verified_via', sa.String(), nullable=True),
    sa.Column('verification_link', sa.String(), nullable=True),
    sa.Column('verification_time', sa.DateTime(), nullable=True),
    sa.Column('date_created', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['order_id'], ['orders.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('ndr_verification',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('order_id', sa.Integer(), nullable=True),
    sa.Column('call_sid', sa.String(), nullable=True),
    sa.Column('recording_url', sa.String(), nullable=True),
    sa.Column('ndr_verified', sa.BOOLEAN(), nullable=True),
    sa.Column('verified_via', sa.String(), nullable=True),
    sa.Column('verification_link', sa.String(), nullable=True),
    sa.Column('verification_time', sa.DateTime(), nullable=True),
    sa.Column('date_created', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['order_id'], ['orders.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('ndr_verification')
    op.drop_table('delivery_check')
    # ### end Alembic commands ###