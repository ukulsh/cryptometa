"""empty message

Revision ID: 201f2b4c0a89
Revises: b829aa1c2605
Create Date: 2021-02-28 19:54:02.142322

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '201f2b4c0a89'
down_revision = 'b829aa1c2605'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('ndr_shipments', sa.Column('defer_dd', sa.DateTime(), nullable=True))
    op.add_column('ndr_shipments', sa.Column('updated_add', sa.String(), nullable=True))
    op.add_column('ndr_shipments', sa.Column('updated_phone', sa.String(), nullable=True))
    op.create_unique_constraint(None, 'thirdwatch_data', ['order_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'thirdwatch_data', type_='unique')
    op.drop_column('ndr_shipments', 'updated_phone')
    op.drop_column('ndr_shipments', 'updated_add')
    op.drop_column('ndr_shipments', 'defer_dd')
    # ### end Alembic commands ###