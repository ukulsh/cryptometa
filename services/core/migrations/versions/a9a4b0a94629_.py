"""empty message

Revision ID: a9a4b0a94629
Revises: 9e13a00bf121
Create Date: 2020-10-12 14:41:05.108438

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a9a4b0a94629'
down_revision = '9e13a00bf121'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('client_default_cost',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('courier_id', sa.Integer(), nullable=True),
    sa.Column('zone_a', sa.FLOAT(), nullable=True),
    sa.Column('zone_b', sa.FLOAT(), nullable=True),
    sa.Column('zone_c', sa.FLOAT(), nullable=True),
    sa.Column('zone_d', sa.FLOAT(), nullable=True),
    sa.Column('zone_e', sa.FLOAT(), nullable=True),
    sa.Column('a_step', sa.FLOAT(), nullable=True),
    sa.Column('b_step', sa.FLOAT(), nullable=True),
    sa.Column('c_step', sa.FLOAT(), nullable=True),
    sa.Column('d_step', sa.FLOAT(), nullable=True),
    sa.Column('e_step', sa.FLOAT(), nullable=True),
    sa.Column('cod_min', sa.FLOAT(), nullable=True),
    sa.Column('cod_ratio', sa.FLOAT(), nullable=True),
    sa.Column('rto_ratio', sa.FLOAT(), nullable=True),
    sa.Column('rvp_ratio', sa.FLOAT(), nullable=True),
    sa.Column('management_fee', sa.FLOAT(), nullable=True),
    sa.ForeignKeyConstraint(['courier_id'], ['master_couriers.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('client_default_cost')
    # ### end Alembic commands ###
