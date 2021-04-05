"""empty message

Revision ID: 488dd4113873
Revises: b788eccc223b
Create Date: 2021-02-11 22:29:35.783698

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '488dd4113873'
down_revision = 'b788eccc223b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('inventory_update_product_id_fkey', 'inventory_update', type_='foreignkey')
    op.create_foreign_key(None, 'inventory_update', 'master_products', ['product_id'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'inventory_update', type_='foreignkey')
    op.create_foreign_key('inventory_update_product_id_fkey', 'inventory_update', 'products', ['product_id'], ['id'])
    # ### end Alembic commands ###