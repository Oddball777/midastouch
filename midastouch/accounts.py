import hashlib
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from platformdirs import user_data_dir
from sqlalchemy import DateTime, Float, String, create_engine, func
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def generate_hash_id(description, date, deposit, withdrawal, balance):
    """
    Generate a unique hash identifier for a transaction.

    Parameters
    ----------
    description : str
        A description of the transaction.
    date : datetime
        The date (and optionally time) of the transaction.
    deposit : float, optional
        The amount deposited. Use None if the transaction was a withdrawal.
    withdrawal : float, optional
        The amount withdrawn. Use None if the transaction was a deposit.
    balance : float
        The balance after the transaction.

    Returns
    -------
    str
        A unique hash identifier for the transaction.
    """
    date_str = date.strftime("%Y-%m-%d %H:%M:%S")
    identifier = f"{description}:{date_str}:{deposit}:{withdrawal}:{balance}"
    return hashlib.sha256(identifier.encode()).hexdigest()


class Base(DeclarativeBase):
    pass


class DebitTransaction(Base):
    """
    A class to represent a transaction in a debit bank account.

    Attributes
    ----------
    id : str
        A unique identifier for the transaction.
    description : str
        A description of the transaction.
    date : datetime
        The date (and optionally time) of the transaction.
    deposit : float, optional
        The amount deposited. Use None if the transaction was a withdrawal.
    withdrawal : float, optional
        The amount withdrawn. Use None if the transaction was a deposit.
    balance : float
        The balance after the transaction.
    """

    __tablename__ = "debit_transactions"

    id: Mapped[str] = mapped_column(String(), primary_key=True, nullable=False)
    description: Mapped[str] = mapped_column(String(), nullable=False)
    date: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    deposit: Mapped[float] = mapped_column(Float(), nullable=True)
    withdrawal: Mapped[float] = mapped_column(Float(), nullable=True)
    balance: Mapped[float] = mapped_column(Float(), nullable=False)

    def __repr__(self):
        rep = (
            f"Transaction(name={self.description!r}, "
            f"date={self.date.strftime('%Y-%m-%d %H:%M:%S')}, "
        )
        if self.deposit is not None:
            rep += f"deposit={self.deposit!r}, "
        if self.withdrawal is not None:
            rep += f"withdrawal={self.withdrawal!r}, "
        rep += f"balance={self.balance!r})"
        return rep


class CreditTransaction(Base):
    """
    A class to represent a transaction in a credit bank account. Credit accounts don't have deposits or withdrawals. Instead, they have charges and payments.

    Attributes
    ----------
    id : str
        A unique identifier for the transaction.
    description : str
        A description of the transaction.
    date : datetime
        The date (and optionally time) of the transaction.
    charge : float, optional
        The amount charged. Use None if the transaction was a payment.
    payment : float, optional
        The amount paid. Use None if the transaction was a charge.
    balance : float
        The balance after the transaction.
    """

    __tablename__ = "credit_transactions"

    id: Mapped[str] = mapped_column(String(), primary_key=True, nullable=False)
    description: Mapped[str] = mapped_column(String(), nullable=False)
    date: Mapped[datetime] = mapped_column(DateTime(), nullable=False)
    charge: Mapped[float] = mapped_column(Float(), nullable=True)
    payment: Mapped[float] = mapped_column(Float(), nullable=True)
    balance: Mapped[float] = mapped_column(Float(), nullable=False)

    def __repr__(self):
        rep = (
            f"Transaction(name={self.description!r}, "
            f"date={self.date.strftime('%Y-%m-%d %H:%M:%S')}, "
        )
        if self.charge is not None:
            rep += f"charge={self.charge!r}, "
        if self.payment is not None:
            rep += f"payment={self.payment!r}, "
        rep += f"balance={self.balance!r})"
        return rep


class DebitAccount:
    def __init__(self, name, create=False):
        """
        An account object that stores transaction data in a SQLite database.

        Parameters
        ----------
        name : str
            The name of the account. Used to create and/or access the database file.
        """
        self.name = name
        directory = user_data_dir("bankdata", roaming=True, ensure_exists=True)
        debit_dir = Path(directory) / "debit"
        debit_dir.mkdir(exist_ok=True)
        db_path = debit_dir / f"{name}.db"
        if not db_path.exists() and not create:
            raise FileNotFoundError(
                f"Debit account '{name}' does not exist. Use create=True to create it."
            )
        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    @classmethod
    def get_all_account_names(cls) -> list[str]:
        """
        Get the names of all accounts in the database.

        Returns
        -------
        list[str]
            A list of the names of all accounts in the database.
        """
        directory = user_data_dir("bankdata", roaming=True, ensure_exists=True)
        debit_dir = Path(directory) / "debit"
        debit_dir.mkdir(exist_ok=True)
        db_files = Path(directory).glob("debit/*.db")
        return [file.stem for file in db_files]

    @classmethod
    def delete_account(cls, name):
        """
        Delete an account from the database.

        Parameters
        ----------
        name : str
            The name of the account to delete.
        """
        directory = user_data_dir("bankdata", roaming=True, ensure_exists=True)
        debit_dir = Path(directory) / "debit"
        debit_dir.mkdir(exist_ok=True)
        db_path = debit_dir / f"{name}.db"
        # Check if account exists
        if not db_path.exists():
            print(f"{name} does not exist")
            return
        # Ask user to type full name of account to confirm deletion
        if (
            input(
                f"Are you sure you want to delete {name}? Type full account name to confirm: "
            )
            == name
        ):
            os.remove(db_path)
            print(f"{name} has been deleted")
        else:
            print(f"{name} was not deleted")

    def _add_transaction(self, description, date, deposit, withdrawal, balance):
        """
        Add a transaction to the database.

        Parameters
        ----------
        description : str
            A description of the transaction.
        date : datetime
            The date (and optionally time) of the transaction.
        deposit : float, optional
            The amount deposited. Use None if the transaction was a withdrawal.
        withdrawal : float, optional
            The amount withdrawn. Use None if the transaction was a deposit.
        balance : float
            The balance after the transaction.
        """
        id = generate_hash_id(description, date, deposit, withdrawal, balance)
        if self.session.query(DebitTransaction).filter_by(id=id).first() is not None:
            return
        transaction = DebitTransaction(
            id=id,
            description=description,
            date=date,
            deposit=deposit,
            withdrawal=withdrawal,
            balance=balance,
        )
        self.session.add(transaction)
        self.session.commit()

    def add_data(self, file_path):
        """
        Add transaction data from a CSV file to the database.

        Parameters
        ----------
        file_path : str
            The path to the CSV file. The file must have columns, in order (no header):
            - date: datetime
            - description: str
            - withdrawal: float
            - deposit: float
            - balance: float
        """
        data = self._load_csv_data(file_path)
        self._update_db_from_data(data)

    def _load_csv_data(self, file_path) -> pd.DataFrame:
        """
        Load transaction data from a CSV file.

        Parameters
        ----------
        file_path : str
            The path to the CSV file. The file must have columns, in order (no header):
            - date: datetime
            - description: str
            - withdrawal: float
            - deposit: float
            - balance: float

        Returns
        -------
        pd.DataFrame
            A DataFrame containing transaction data.
        """
        try:
            data = pd.read_csv(file_path, header=None)
            data.columns = ["date", "description", "withdrawal", "deposit", "balance"]
            data["date"] = pd.to_datetime(data["date"])
        except UnicodeDecodeError:
            data = pd.read_csv(file_path, header=None, encoding="latin1")
            # keep only columns 2, 3, 5, 7, 8, 13
            data = data.iloc[:, [2, 3, 5, 7, 8, 13]]
            data.columns = [
                "account",
                "date",
                "description",
                "withdrawal",
                "deposit",
                "balance",
            ]
            # find most common account name and keep only rows with that account name
            account_name = data["account"].mode()[0]
            data = data[data["account"] == account_name]
            data = data.drop(columns=["account"])
            data["date"] = pd.to_datetime(data["date"])
        # check if dates are in increasing order
        if not data["date"].is_monotonic_increasing:
            # reverse the order of the DataFrame
            data = data.iloc[::-1]
        return data

    def _update_db_from_data(self, data: pd.DataFrame):
        """
        Update the database with data from a DataFrame.

        Parameters
        ----------
        data : pd.DataFrame
            A DataFrame containing transaction data. Must have columns:
            - description: str
            - date: datetime
            - deposit: float
            - withdrawal: float
            - balance: float
        """
        for _, row in data.iterrows():
            self._add_transaction(
                description=row["description"],
                date=row["date"],
                deposit=row["deposit"],
                withdrawal=row["withdrawal"],
                balance=row["balance"],
            )

    def get_transactions(
        self,
        deposits=True,
        withdrawals=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ):
        """
        Get all transactions in the database.

        Parameters
        ----------
        deposits : bool, optional
            Whether to include deposits in the results.
        withdrawals : bool, optional
            Whether to include withdrawals in the results.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
            The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        list[Transaction]
            A list of all transactions in the database that match the criteria.
        """
        query = self.session.query(DebitTransaction)
        if not deposits:
            query = query.filter(DebitTransaction.deposit.is_(None))
        if not withdrawals:
            query = query.filter(DebitTransaction.withdrawal.is_(None))
        if date_start is not None:
            query = query.filter(DebitTransaction.date >= date_start)
        if date_end is not None:
            query = query.filter(DebitTransaction.date <= date_end)
        if description_contains is not None:
            query = query.filter(
                DebitTransaction.description.contains(description_contains)
            )
        query = query.order_by(DebitTransaction.date)
        return query.all()

    def get_balance(self) -> float:
        """
        Get the current balance of the account.

        Returns
        -------
        float
            The current balance of the account.
        """
        first_transaction = self.get_transactions()[-1]
        if first_transaction is None:
            raise ValueError("No transactions found")
        return first_transaction.balance

    def count_transactions(
        self,
        deposits=True,
        withdrawals=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ) -> int:
        """
        Get the number of transactions in the database.

        Parameters
        ----------
        deposits : bool, optional
            Whether to include deposits in the count.
        withdrawals : bool, optional
            Whether to include withdrawals in the count.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
            The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        int
            The number of transactions in the database that match the criteria.
        """
        query = self.session.query(func.count(DebitTransaction.id))
        if not deposits:
            query = query.filter(DebitTransaction.deposit.is_(None))
        if not withdrawals:
            query = query.filter(DebitTransaction.withdrawal.is_(None))
        if date_start is not None:
            query = query.filter(DebitTransaction.date >= date_start)
        if date_end is not None:
            query = query.filter(DebitTransaction.date <= date_end)
        if description_contains is not None:
            query = query.filter(
                DebitTransaction.description.contains(description_contains)
            )
        return query.scalar()

    def sum_transactions(
        self,
        deposits=True,
        withdrawals=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ):
        """
        Get the sum of all transactions which match the specified criteria.

        Parameters
        ----------
        deposits : bool, optional
            Whether to include deposits in the sum.
        withdrawals : bool, optional
            Whether to include withdrawals in the sum.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
            The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        float
            The sum of all transactions that contain the specified string.
        """
        query = self.session.query(
            func.sum(DebitTransaction.deposit), func.sum(DebitTransaction.withdrawal)
        )
        if not deposits:
            query = query.filter(DebitTransaction.deposit.is_(None))
        if not withdrawals:
            query = query.filter(DebitTransaction.withdrawal.is_(None))
        if date_start is not None:
            query = query.filter(DebitTransaction.date >= date_start)
        if date_end is not None:
            query = query.filter(DebitTransaction.date <= date_end)
        if description_contains is not None:
            query = query.filter(
                DebitTransaction.description.contains(description_contains)
            )
        deposit, withdrawal = query.one()
        return round((deposit or 0) - (withdrawal or 0), 2)

    def average_transactions(
        self,
        deposits=True,
        withdrawals=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ):
        """
        Get the average of all transactions which match the specified criteria.

        Parameters
        ----------
        deposits : bool, optional
            Whether to include deposits in the average.
        withdrawals : bool, optional
            Whether to include withdrawals in the average.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
            The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        float
            The average value of all transactions that match the criteria.
        """
        if (
            self.count_transactions(
                deposits, withdrawals, date_start, date_end, description_contains
            )
            == 0
        ):
            raise ValueError("No transactions found for the specified criteria.")
        total = self.sum_transactions(
            deposits, withdrawals, date_start, date_end, description_contains
        )
        count = self.count_transactions(
            deposits, withdrawals, date_start, date_end, description_contains
        )
        return total / count

    def check_validity(self):
        """
        Check if the transactions in the database are valid.

        Returns
        -------
        bool
            True if the transactions are valid, False otherwise.
        """
        if self.count_transactions() == 0:
            print("This account has no transactions.")
            return True
        total_transactions = round(self.sum_transactions(), 2)
        first_transaction = self.get_transactions()[0]
        # first balance is actually the balance AFTER the first transaction, so we need to remove the first transaction amount
        if first_transaction.deposit is not None:
            first_balance = first_transaction.balance - first_transaction.deposit
        else:
            first_balance = first_transaction.balance + first_transaction.withdrawal
        last_balance = self.get_balance()

        diff_balance = round(last_balance - first_balance, 2)

        # check if differences are equal
        return total_transactions == diff_balance

    def close_session(self):
        """
        Close the database session.
        """
        self.session.close()


class CreditAccount:
    def __init__(self, name, create=False):
        """
        An account object that stores transaction data in a SQLite database.

        Parameters
        ----------
        name : str
            The name of the account. Used to create and/or access the database file.
        """
        self.name = name
        directory = user_data_dir("bankdata", roaming=True, ensure_exists=True)
        credit_dir = Path(directory) / "credit"
        credit_dir.mkdir(exist_ok=True)
        db_path = credit_dir / f"{name}.db"
        if not db_path.exists() and not create:
            raise FileNotFoundError(
                f"Credit account '{name}' does not exist. Use create=True to create it."
            )
        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    @classmethod
    def get_all_account_names(cls) -> list[str]:
        """
        Get the names of all accounts in the database.

        Returns
        -------
        list[str]
            A list of the names of all accounts in the database.
        """
        directory = user_data_dir("bankdata", roaming=True, ensure_exists=True)
        db_files = Path(directory).glob("*.db")
        return [file.stem for file in db_files]

    @classmethod
    def delete_account(cls, name):
        """
        Delete an account from the database.

        Parameters
        ----------
        name : str
            The name of the account to delete.
        """
        directory = user_data_dir("bankdata", roaming=True, ensure_exists=True)
        db_path = Path(directory) / f"{name}.db"
        # Check if account exists
        if not db_path.exists():
            print(f"{name} does not exist")
            return
        # Ask user to type full name of account to confirm deletion
        if (
            input(
                f"Are you sure you want to delete {name}? Type full account name to confirm: "
            )
            == name
        ):
            os.remove(db_path)
            print(f"{name} has been deleted")
        else:
            print(f"{name} was not deleted")

    def _add_transaction(self, description, date, charge, payment, balance):
        """
        Add a transaction to the database.

        Parameters
        ----------
        description : str
            A description of the transaction.
        date : datetime
            The date (and optionally time) of the transaction.
        charge : float, optional
            The amount charged. Use None if the transaction was a payment.
        payment : float, optional
            The amount paid. Use None if the transaction was a charge.
        balance : float
            The balance after the transaction.
        """
        id = generate_hash_id(description, date, charge, payment, balance)
        if self.session.query(CreditTransaction).filter_by(id=id).first() is not None:
            return
        transaction = CreditTransaction(
            id=id,
            description=description,
            date=date,
            charge=charge,
            payment=payment,
            balance=balance,
        )
        self.session.add(transaction)
        self.session.commit()

    def add_data(self, file_path):
        """
        Add transaction data from a CSV file to the database.

        Parameters
        ----------
        file_path : str
            The path to the CSV file. The file must have columns, in order (no header):
            - date: datetime
            - description: str
            - charge: float
            - payment: float
            - balance: float
        """
        data = self._load_csv_data(file_path)
        self._update_db_from_data(data)

    def _load_csv_data(self, file_path) -> pd.DataFrame:
        """
        Load transaction data from a CSV file.

        Parameters
        ----------
        file_path : str
            The path to the CSV file. The file must have columns, in order (no header):
            - date: datetime
            - description: str
            - charge: float
            - payment: float
            - balance: float

        Returns
        -------
        pd.DataFrame
            A DataFrame containing transaction data.
        """
        data = pd.read_csv(file_path, header=None)
        data.columns = ["date", "description", "charge", "payment", "balance"]
        data["date"] = pd.to_datetime(data["date"])
        # check if dates are in increasing order
        if not data["date"].is_monotonic_increasing:
            # reverse the order of the DataFrame
            data = data.iloc[::-1]
        return data

    def _update_db_from_data(self, data: pd.DataFrame):
        """
        Update the database with data from a DataFrame.

        Parameters
        ----------
        data : pd.DataFrame
            A DataFrame containing transaction data. Must have columns:
            - description: str
            - date: datetime
            - charge: float
            - payment: float
            - balance: float
        """
        for _, row in data.iterrows():
            self._add_transaction(
                description=row["description"],
                date=row["date"],
                charge=row["charge"],
                payment=row["payment"],
                balance=row["balance"],
            )

    def get_transactions(
        self,
        charges=True,
        payments=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ):
        """
        Get all transactions in the database.

        Parameters
        ----------
        charges : bool, optional
            Whether to include charges in the results.
        payments : bool, optional
            Whether to include payments in the results.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
            The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        list[Transaction]
            A list of all transactions in the database that match the criteria.
        """
        query = self.session.query(CreditTransaction)
        if not charges:
            query = query.filter(CreditTransaction.charge.is_(None))
        if not payments:
            query = query.filter(CreditTransaction.payment.is_(None))
        if date_start is not None:
            query = query.filter(CreditTransaction.date >= date_start)
        if date_end is not None:
            query = query.filter(CreditTransaction.date <= date_end)
        if description_contains is not None:
            query = query.filter(
                CreditTransaction.description.contains(description_contains)
            )
        query = query.order_by(CreditTransaction.date)
        return query.all()

    def get_balance(self) -> float:
        """
        Get the current balance of the account.

        Returns
        -------
        float
            The current balance of the account.
        """
        first_transaction = self.get_transactions()[-1]
        if first_transaction is None:
            raise ValueError("No transactions found")
        return first_transaction.balance

    def count_transactions(
        self,
        charges=True,
        payments=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ) -> int:
        """
        Get the number of transactions in the database.

        Parameters
        ----------
        charges : bool, optional
            Whether to include charges in the count.
        payments : bool, optional
            Whether to include payments in the count.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
            The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        int
            The number of transactions in the database that match the criteria.
        """
        query = self.session.query(func.count(CreditTransaction.id))
        if not charges:
            query = query.filter(CreditTransaction.charge.is_(None))
        if not payments:
            query = query.filter(CreditTransaction.payment.is_(None))
        if date_start is not None:
            query = query.filter(CreditTransaction.date >= date_start)
        if date_end is not None:
            query = query.filter(CreditTransaction.date <= date_end)
        if description_contains is not None:
            query = query.filter(
                CreditTransaction.description.contains(description_contains)
            )
        return query.scalar()

    def sum_transactions(
        self,
        charges=True,
        payments=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ):
        """
        Get the sum of all transactions which match the specified criteria.

        Parameters
        ----------
        charges : bool, optional
            Whether to include charges in the sum.
        payments : bool, optional
            Whether to include payments in the sum.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
            The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        float
            The sum of all transactions that contain the specified string.
        """
        query = self.session.query(
            func.sum(CreditTransaction.charge), func.sum(CreditTransaction.payment)
        )
        if not charges:
            query = query.filter(CreditTransaction.charge.is_(None))
        if not payments:
            query = query.filter(CreditTransaction.payment.is_(None))
        if date_start is not None:
            query = query.filter(CreditTransaction.date >= date_start)
        if date_end is not None:
            query = query.filter(CreditTransaction.date <= date_end)
        if description_contains is not None:
            query = query.filter(
                CreditTransaction.description.contains(description_contains)
            )
        charge, payment = query.one()
        return (charge or 0) - (payment or 0)

    def average_transactions(
        self,
        charges=True,
        payments=True,
        date_start=None,
        date_end=None,
        description_contains=None,
    ):
        """
        Get the average of all transactions which match the specified criteria.

        Parameters
        ----------
        charges : bool, optional
            Whether to include charges in the average.
        payments : bool, optional
            Whether to include payments in the average.
        date_start : datetime, optional
            The start date to filter transactions.
        date_end : datetime, optional
        The end date to filter transactions.
        description_contains : str, optional
            A string to search for in the transaction descriptions.

        Returns
        -------
        float
            The average value of all transactions that match the criteria.
        """
        total = self.sum_transactions(
            charges, payments, date_start, date_end, description_contains
        )
        count = self.count_transactions(
            charges, payments, date_start, date_end, description_contains
        )
        return total / count

    def check_validity(self):
        """
        Check if the transactions in the database are valid.

        Returns
        -------
        bool
            True if the transactions are valid, False otherwise.
        """
        total_transactions = round(self.sum_transactions(), 2)
        first_transaction = self.get_transactions()[0]
        # first balance is actually the balance AFTER the first transaction, so we need to remove the first transaction amount
        if first_transaction.charge is not None:
            first_balance = first_transaction.balance - first_transaction.charge
        else:
            first_balance = first_transaction.balance + first_transaction.payment
        last_balance = self.get_balance()

        diff_balance = round(last_balance - first_balance, 2)

        # check if differences are equal
        return total_transactions == diff_balance

    def close_session(self):
        """
        Close the database session.
        """
        self.session.close()


def main() -> None:
    gc = DebitAccount("test", create=True)

    # check validity of transactions
    print(gc.check_validity())


if __name__ == "__main__":
    main()
