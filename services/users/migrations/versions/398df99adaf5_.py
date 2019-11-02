"""empty message

Revision ID: 398df99adaf5
Revises: 71c798076933
Create Date: 2019-11-02 11:00:22.188469

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '398df99adaf5'
down_revision = '71c798076933'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('users', sa.Column('admin', sa.Boolean(), nullable=True))
    op.execute('UPDATE users SET admin=False')
    op.alter_column('users', 'admin', nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('users', 'admin')
    # ### end Alembic commands ###
