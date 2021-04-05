"""empty message

Revision ID: 501b695f6b9b
Revises: 43cf9eea4004
Create Date: 2021-01-27 21:36:02.858755

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '501b695f6b9b'
down_revision = '43cf9eea4004'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('master_products',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('sku', sa.String(), nullable=False),
    sa.Column('dimensions', postgresql.JSON(astext_type=sa.Text()), nullable=True),
    sa.Column('weight', sa.FLOAT(), nullable=True),
    sa.Column('product_image', sa.String(), nullable=True),
    sa.Column('price', sa.FLOAT(), nullable=True),
    sa.Column('client_prefix', sa.String(), nullable=True),
    sa.Column('active', sa.BOOLEAN(), nullable=True),
    sa.Column('subcategory_id', sa.Integer(), nullable=True),
    sa.Column('hsn_code', sa.String(), nullable=True),
    sa.Column('tax_rate', sa.FLOAT(), nullable=True),
    sa.Column('date_created', sa.DateTime(), nullable=True),
    sa.Column('date_updated', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['subcategory_id'], ['products_subcategories.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.add_column('products', sa.Column('master_product_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'products', 'master_products', ['master_product_id'], ['id'])
    op.drop_column('products', 'active')
    op.drop_column('products', 'inactive_reason')
    op.add_column('products_quantity', sa.Column('wh_loc', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('products_quantity', 'wh_loc')
    op.add_column('products', sa.Column('inactive_reason', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.add_column('products', sa.Column('active', sa.BOOLEAN(), autoincrement=False, nullable=True))
    op.drop_constraint(None, 'products', type_='foreignkey')
    op.drop_column('products', 'master_product_id')
    op.drop_table('master_products')
    # ### end Alembic commands ###