"""empty message

Revision ID: 5427239fa892
Revises: 3e838d04da2c
Create Date: 2020-05-18 20:27:38.165715

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '5427239fa892'
down_revision = '3e838d04da2c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('client_pickups', sa.Column('gstin', sa.String(), nullable=True))
    op.add_column('op_association', sa.Column('tax_lines', postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.add_column('products', sa.Column('hsn_code', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('products', 'hsn_code')
    op.drop_column('op_association', 'tax_lines')
    op.drop_column('client_pickups', 'gstin')
    # ### end Alembic commands ###