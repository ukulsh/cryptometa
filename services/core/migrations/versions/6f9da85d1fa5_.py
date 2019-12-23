"""empty message

Revision ID: 6f9da85d1fa5
Revises: b1eda94cffd7
Create Date: 2019-12-19 20:43:17.792115

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6f9da85d1fa5'
down_revision = 'b1eda94cffd7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('order_status',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('order_id', sa.Integer(), nullable=True),
    sa.Column('courier_id', sa.Integer(), nullable=True),
    sa.Column('status_code', sa.String(), nullable=True),
    sa.Column('status', sa.String(), nullable=True),
    sa.Column('status_text', sa.String(), nullable=True),
    sa.Column('location', sa.String(), nullable=True),
    sa.Column('status_time', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['courier_id'], ['master_couriers.id'], ),
    sa.ForeignKeyConstraint(['order_id'], ['orders.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.drop_column('manifests', 'no_of_orders')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('manifests', sa.Column('no_of_orders', sa.INTEGER(), autoincrement=False, nullable=True))
    op.drop_table('order_status')
    # ### end Alembic commands ###
