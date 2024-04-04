import random
from datetime import datetime, timedelta

from sqlalchemy import Column, Date, Float, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Setup SQLAlchemy base and engine
Base = declarative_base()
engine = create_engine("sqlite:///:memory:", echo=False)
Session = sessionmaker(bind=engine)
session = Session()


# Define a simple Transaction model
class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    transaction_date = Column(Date, nullable=False)
    amount = Column(Float, nullable=False)


# Create the table
Base.metadata.create_all(engine)

# Generate random transactions for the last month
last_month_dates = [
    (datetime.now() - timedelta(days=random.randint(0, 30))).date() for _ in range(10)
]
transactions_last_month = [
    Transaction(transaction_date=date, amount=random.uniform(10, 100))
    for date in last_month_dates
]

# Generate random transactions for two months ago
two_months_ago_dates = [
    (datetime.now() - timedelta(days=random.randint(30, 60))).date() for _ in range(5)
]
transactions_two_months_ago = [
    Transaction(transaction_date=date, amount=random.uniform(10, 100))
    for date in two_months_ago_dates
]

# Add and commit transactions to the database
session.add_all(transactions_last_month + transactions_two_months_ago)
session.commit()

# Fetch and print all transactions unsorted
unsorted_transactions = session.query(Transaction).all()

# Fetch and print all transactions sorted
sorted_transactions = (
    session.query(Transaction).order_by(Transaction.transaction_date).all()
)

for i in unsorted_transactions:
    print(i.transaction_date, i.amount)

print("\n")
for i in sorted_transactions:
    print(i.transaction_date, i.amount)
