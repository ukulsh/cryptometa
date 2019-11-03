# services/exercises/project/api/models.py


from project import db


class Products(db.Model):
    __tablename__ = "products"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)
    sku = db.Column(db.String, nullable=False, unique=True)

    def __init__(self, name, sku):
        self.name = name
        self.sku = sku

    def to_json(self):
        return {
            'id': self.id,
            'name': self.name,
            'sku': self.sku
        }