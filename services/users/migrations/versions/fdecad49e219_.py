"""empty message

Revision ID: fdecad49e219
Revises: 2bdb50bf6b00
Create Date: 2020-09-29 14:37:56.160266

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fdecad49e219'
down_revision = '2bdb50bf6b00'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('clients', sa.Column('canceled_cheque_link', sa.String(), nullable=True))
    op.add_column('clients', sa.Column('kyc_verified', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('clients', sa.Column('pan_link', sa.String(), nullable=True))
    op.add_column('clients', sa.Column('signed_agreement_link', sa.String(), nullable=True))
    op.drop_column('clients', 'canceled_check_link')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('clients', sa.Column('canceled_check_link', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.drop_column('clients', 'signed_agreement_link')
    op.drop_column('clients', 'pan_link')
    op.drop_column('clients', 'kyc_verified')
    op.drop_column('clients', 'canceled_cheque_link')
    # ### end Alembic commands ###
