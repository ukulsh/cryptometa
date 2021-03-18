"""empty message

Revision ID: 1388564500b2
Revises: 6796e765c3d9
Create Date: 2020-05-07 14:38:43.721013

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1388564500b2'
down_revision = '6796e765c3d9'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('client_mapping', sa.Column('essential', sa.BOOLEAN(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('client_mapping', 'essential')
    # ### end Alembic commands ###