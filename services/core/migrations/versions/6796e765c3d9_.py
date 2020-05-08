"""empty message

Revision ID: 6796e765c3d9
Revises: 23d8a58148d4
Create Date: 2020-05-02 17:02:51.833855

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6796e765c3d9'
down_revision = '23d8a58148d4'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('inventory_update',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('product_id', sa.Integer(), nullable=True),
    sa.Column('warehouse_prefix', sa.String(), nullable=False),
    sa.Column('user', sa.String(), nullable=False),
    sa.Column('remark', sa.String(), nullable=True),
    sa.Column('quantity', sa.Integer(), nullable=False),
    sa.Column('type', sa.Integer(), nullable=False),
    sa.Column('date_created', sa.DateTime(), nullable=True),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['product_id'], ['products.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('inventory_update')
    # ### end Alembic commands ###
