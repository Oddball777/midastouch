import hashlib
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Self, Union

import pandas as pd
import yaml
from anytree import Node, RenderTree
from platformdirs import user_data_dir
from sqlalchemy import DateTime, Float, String, and_, create_engine, func, or_
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


class CategoryManager:
    def __init__(self):
        self.directory = user_data_dir("midastouch", roaming=True, ensure_exists=True)
        self.yaml_file_path = os.path.join(self.directory, "categories.yml")

        if not os.path.exists(self.yaml_file_path):
            with open(self.yaml_file_path, "w") as yaml_file:
                yaml.dump({}, yaml_file)

        with open(self.yaml_file_path, "r") as yaml_file:
            self.categories = yaml.safe_load(yaml_file) or {}

    def save(self):
        with open(self.yaml_file_path, "w") as yaml_file:
            yaml.dump(self.categories, yaml_file)

    def _find_category(self, name, category=None, path=""):
        if category is None:
            category = self.categories

        if name in category:
            return category, path + name

        for subcat, subcat_content in category.items():
            if isinstance(subcat_content, dict):
                found_category, found_path = self._find_category(
                    name, subcat_content, path + subcat + "/"
                )
                if found_category:
                    return found_category, found_path
        return None, None

    def add_category(self, name, parent_name=None, keywords=None):
        keywords = keywords or []
        if parent_name:
            parent_category, parent_path = self._find_category(parent_name)
            if parent_category is None:
                print(f"Parent category {parent_name} does not exist.")
                return
            parent_category[parent_name][name] = {"_keywords": keywords}
        else:
            self.categories[name] = {"_keywords": keywords}
        self.save()

    def remove_category(self, name):
        category, path = self._find_category(name)
        if category and name in category:
            del category[name]
            self.save()
        else:
            print(f"Category {name} does not exist.")

    def move_category(self, name, new_parent_name):
        category, path = self._find_category(name)
        if category and name in category:
            new_parent_category, new_parent_path = self._find_category(new_parent_name)
            if new_parent_category:
                new_parent_category[new_parent_name][name] = category.pop(name)
                self.save()
            else:
                print(f"Parent category {new_parent_name} does not exist.")
        else:
            print(f"Category {name} does not exist.")

    def add_keywords(self, name, keywords):
        category, path = self._find_category(name)
        if category and name in category:
            if isinstance(keywords, str):
                keywords = [keywords]
            # Add new keywords to the category if they don't already exist
            category[name]["_keywords"] = list(
                set(category[name]["_keywords"] + keywords)
            )
            self.save()
        else:
            print(f"Category {name} does not exist.")

    def remove_keywords(self, name, keywords):
        category, path = self._find_category(name)
        if category and name in category:
            category[name]["_keywords"] = [
                kw for kw in category[name]["_keywords"] if kw not in keywords
            ]
            self.save()
        else:
            print(f"Category {name} does not exist.")

    def rename_category(self, old_name, new_name):
        category, path = self._find_category(old_name)
        if category and old_name in category:
            category[new_name] = category.pop(old_name)
            self.save()
        else:
            print(f"Category {old_name} does not exist.")

    def get_all_categories(self):
        return self.categories

    def _build_tree(self, name, parent, category, show_keywords):
        if show_keywords and "_keywords" in category:
            keywords_str = ", ".join(category["_keywords"])
            node_name = f"{name} (Keywords: {keywords_str})"
        else:
            node_name = name
        node = Node(node_name, parent=parent)
        for subcat, subcat_content in category.items():
            if subcat != "_keywords":
                self._build_tree(subcat, node, subcat_content, show_keywords)
        return node

    def visualize_categories(self, show_keywords=False):
        root = Node("Categories")
        for name, content in self.categories.items():
            self._build_tree(name, root, content, show_keywords)
        for pre, fill, node in RenderTree(root):
            print("%s%s" % (pre, node.name))

    def _get_keywords_recursive(self, category):
        keywords = category.get("_keywords", [])
        for subcat, subcat_content in category.items():
            if isinstance(subcat_content, dict):
                keywords.extend(self._get_keywords_recursive(subcat_content))
        return keywords

    def get_keywords(self, name, include_subcategories=True):
        category, path = self._find_category(name)
        if category and name in category:
            if include_subcategories:
                return self._get_keywords_recursive(category[name])
            else:
                return category[name].get("_keywords", [])
        else:
            print(f"Category {name} does not exist.")
            return []


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
        directory = user_data_dir("midastouch", roaming=True, ensure_exists=True)
        debit_dir = Path(directory) / "debit"
        debit_dir.mkdir(exist_ok=True)
        db_path = debit_dir / f"{name}.db"
        if not db_path.exists() and not create:
            raise FileNotFoundError(
                f"Debit account '{name}' does not exist. Use create=True to create it."
            )
        if create and db_path.exists():
            raise FileExistsError(
                f"Debit account '{name}' already exists. Use create=False (default) to access it or choose a different name."
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
        directory = user_data_dir("midastouch", roaming=True, ensure_exists=True)
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
        directory = user_data_dir("midastouch", roaming=True, ensure_exists=True)
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

    def _add_transaction(
        self,
        description: str,
        date: datetime,
        deposit: Optional[float],
        withdrawal: Optional[float],
        balance: float,
    ):
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
        # Make sure accent errors are not present
        description = description.replace("Ã©", "e")
        id = generate_hash_id(
            description=description,
            date=date,
            deposit=deposit,
            withdrawal=withdrawal,
            balance=balance,
        )
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
            # replace multiple spaces with single space
            data["description"] = data["description"].apply(
                lambda x: re.sub(" +", " ", x)
            )
            data["description"] = data["description"].str.ljust(20)
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

    def get_balance(self) -> float:
        """
        Get the current balance of the account.

        Returns
        -------
        float
            The current balance of the account.
        """
        latest_transaction = self.query().transactions(as_list=True)[-1]
        if latest_transaction is None:
            raise ValueError("No transactions found")
        return latest_transaction.balance

    def check_validity(self):
        """
        Check if the transactions in the database are valid.

        Returns
        -------
        bool
            True if the transactions are valid, False otherwise.
        """
        queryer = self.query()
        if queryer.count() == 0:
            print("This account has no transactions.")
            return True
        total_transactions = round(queryer.sum(), 2)
        first_transaction = queryer.transactions(as_list=True, ascending=True)[0]
        # first balance is actually the balance AFTER the first transaction, so we need to remove the first transaction amount
        if first_transaction.deposit is not None:
            first_balance = first_transaction.balance - first_transaction.deposit
        else:
            first_balance = first_transaction.balance + first_transaction.withdrawal
        last_balance = self.get_balance()

        diff_balance = round(last_balance - first_balance, 2)

        # check if differences are equal
        print(f"First balance: {first_balance}")
        print(f"Last balance: {last_balance}")
        print(f"Total transactions: {total_transactions}")
        print(f"Difference in balance: {diff_balance}")
        print(total_transactions == diff_balance)
        return total_transactions == diff_balance

    def clean_data(self):
        """
        Find possible duplicates in the database and ask the user which ones to remove if any.
        """
        queryer = self.query()
        transactions = queryer.transactions(as_list=True)
        duplicates = []
        # A match is found if date, balance, deposit, and withdrawal are the same. Description can be different.
        for i, txn1 in enumerate(transactions):
            for j, txn2 in enumerate(transactions):
                if i != j and txn1.date == txn2.date and txn1.balance == txn2.balance:
                    if (
                        txn1.deposit == txn2.deposit
                        and txn1.withdrawal == txn2.withdrawal
                    ):
                        if (i, j) not in duplicates and (j, i) not in duplicates:
                            duplicates.append((i, j))
                            break
        if duplicates:
            print("Possible duplicates found:")
            for i, j in duplicates:
                print(f"Transaction {i}: {transactions[i]}")
                print(f"Transaction {j}: {transactions[j]}")
                print()
            if input("Do you want to remove duplicates? (y/n): ").lower() == "y":
                to_remove = input("Enter the indices of the transactions you want to remove (separated by a comma): ")
                to_remove = [int(i) for i in to_remove.split(",")]
                for i in to_remove:
                    self.session.delete(transactions[i])
                self.session.commit()
                print("Duplicates removed.")
                
    
    def check_validity_in_range(self, date_start, date_end):
        """
        Check if the transactions in the database are valid within a specified date range.

        Parameters
        ----------
        date_start : datetime
            The start date of the date range.
        date_end : datetime
            The end date of the date range.

        Returns
        -------
        bool
            True if the transactions are valid, False otherwise.
        """
        queryer = self.query()
        if queryer.count() == 0:
            print("This account has no transactions.")
            return True
        total_transactions = round(
            queryer.filter_date_range(date_start, date_end).sum(), 2
        )
        first_transaction = queryer.transactions(as_list=True, ascending=True)[0]
        # first balance is actually the balance AFTER the first transaction, so we need to remove the first transaction amount
        if first_transaction.deposit is not None:
            first_balance = first_transaction.balance - first_transaction.deposit
        else:
            first_balance = first_transaction.balance + first_transaction.withdrawal
        last_transaction = queryer.transactions(as_list=True, ascending=True)[-1]
        last_balance = last_transaction.balance

        diff_balance = round(last_balance - first_balance, 2)

        # check if differences are equal
        print(f"First balance: {first_balance}")
        print(f"Last balance: {last_balance}")
        print(f"Total transactions: {total_transactions}")
        print(f"Difference in balance: {diff_balance}")
        print(total_transactions == diff_balance)
        return total_transactions == diff_balance

    def close_session(self):
        """
        Close the database session.
        """
        self.session.close()

    def query(self):
        """
        Execute a custom query on the database.

        Parameters
        ----------
        query_str : str
            The query string to execute.
        """

        return TransactionQuery(self.session, DebitTransaction)


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
        directory = user_data_dir("midastouch", roaming=True, ensure_exists=True)
        credit_dir = Path(directory) / "credit"
        credit_dir.mkdir(exist_ok=True)
        db_path = credit_dir / f"{name}.db"
        if not db_path.exists() and not create:
            raise FileNotFoundError(
                f"Credit account '{name}' does not exist. Use create=True to create it."
            )
        if create and db_path.exists():
            raise FileExistsError(
                f"Credit account '{name}' already exists. Use create=False (default) to access it or choose a different name."
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
        directory = user_data_dir("midastouch", roaming=True, ensure_exists=True)
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
        directory = user_data_dir("midastouch", roaming=True, ensure_exists=True)
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

    def _add_transaction(
        self,
        description: str,
        date: datetime,
        charge: Optional[float],
        payment: Optional[float],
        balance: float,
    ):
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
        id = generate_hash_id(
            description=description,
            date=date,
            deposit=charge,
            withdrawal=payment,
            balance=balance,
        )
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
            - date (and optionally time): str (format: "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD")
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
            - date (and optionally time): str (format: "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD")
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

    def get_balance(self) -> float:
        """
        Get the current balance of the account.

        Returns
        -------
        float
            The current balance of the account.
        """
        most_recent_transaction = self.query().transactions(
            as_list=True,
        )[-1]
        if most_recent_transaction is None:
            raise ValueError("No transactions found")
        return most_recent_transaction.balance

    def check_validity(self):
        """
        Check if the transactions in the database are valid.

        Returns
        -------
        bool
            True if the transactions are valid, False otherwise.
        """
        total_transactions = round(self.query().sum(), 2)
        first_transaction = self.query().transactions(as_list=True)[0]
        # first balance is actually the balance AFTER the first transaction, so we need to remove the first transaction amount
        if first_transaction.charge is not None:
            first_balance = first_transaction.balance - first_transaction.charge
        else:
            first_balance = first_transaction.balance + first_transaction.payment
        last_balance = self.get_balance()

        diff_balance = round(last_balance - first_balance, 2)

        print(total_transactions == diff_balance)
        return total_transactions == diff_balance

    def close_session(self):
        """
        Close the database session.
        """
        self.session.close()

    def query(self):
        """
        Execute a custom query on the database.

        Parameters
        ----------
        query_str : str
            The query string to execute.
        """

        return TransactionQuery(self.session, CreditTransaction)


class TransactionQuery:
    def __init__(self, session, transaction_type):
        self.session = session
        self.transaction_type = transaction_type
        self.query = session.query(self.transaction_type)
        self.group_by_attr = None

    def filter_transactions(self, include: bool, transaction_field):
        if not include:
            self.query = self.query.filter(transaction_field.is_(None))
        return self

    def filter_deposits(self, include: bool):
        if self.transaction_type == DebitTransaction:
            return self.filter_transactions(include, DebitTransaction.deposit)
        return self

    def filter_withdrawals(self, include: bool):
        if self.transaction_type == DebitTransaction:
            return self.filter_transactions(include, DebitTransaction.withdrawal)
        return self

    def filter_charges(self, include: bool):
        if self.transaction_type == CreditTransaction:
            return self.filter_transactions(include, CreditTransaction.charge)
        return self

    def filter_payments(self, include: bool):
        if self.transaction_type == CreditTransaction:
            return self.filter_transactions(include, CreditTransaction.payment)
        return self

    def filter_date_range(
        self,
        date_start: Optional[Union[datetime, str]] = None,
        date_end: Optional[Union[datetime, str]] = None,
        invert: bool = False,
    ):
        if isinstance(date_start, str):
            date_start = datetime.fromisoformat(date_start)
        if isinstance(date_end, str):
            date_end = datetime.fromisoformat(date_end)

        if date_start is not None and date_end is not None:
            if invert:
                self.query = self.query.filter(
                    or_(
                        self.transaction_type.date < date_start,
                        self.transaction_type.date > date_end,
                    )
                )
            else:
                self.query = self.query.filter(
                    self.transaction_type.date >= date_start,
                    self.transaction_type.date <= date_end,
                )
        elif date_start is not None:
            if invert:
                self.query = self.query.filter(self.transaction_type.date < date_start)
            else:
                self.query = self.query.filter(self.transaction_type.date >= date_start)
        elif date_end is not None:
            if invert:
                self.query = self.query.filter(self.transaction_type.date > date_end)
            else:
                self.query = self.query.filter(self.transaction_type.date <= date_end)

        return self

    def filter_description(
        self,
        description_contains: Optional[Union[str, list[str]]] = None,
        invert: bool = False,
    ):
        if description_contains is not None:
            if isinstance(description_contains, str):
                if invert:
                    self.query = self.query.filter(
                        ~self.transaction_type.description.contains(
                            description_contains
                        )
                    )
                else:
                    self.query = self.query.filter(
                        self.transaction_type.description.contains(description_contains)
                    )
            elif isinstance(description_contains, list):
                if invert:
                    self.query = self.query.filter(
                        ~or_(
                            *[
                                self.transaction_type.description.contains(keyword)
                                for keyword in description_contains
                            ]
                        )
                    )
                else:
                    self.query = self.query.filter(
                        or_(
                            *[
                                self.transaction_type.description.contains(keyword)
                                for keyword in description_contains
                            ]
                        )
                    )
            else:
                raise ValueError(
                    "description_contains must be a string or a list of strings. Use the output of the categories(name: str) function to filter by categories."
                )
        return self

    def filter_amount(
        self,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        invert: bool = False,
    ):
        if self.transaction_type == DebitTransaction:
            deposit_col = DebitTransaction.deposit
            withdrawal_col = DebitTransaction.withdrawal
        elif self.transaction_type == CreditTransaction:
            deposit_col = CreditTransaction.charge
            withdrawal_col = CreditTransaction.payment

        if min_amount is not None and max_amount is not None:
            if invert:
                self.query = self.query.filter(
                    or_(
                        deposit_col < min_amount,
                        deposit_col > max_amount,
                        withdrawal_col < min_amount,
                        withdrawal_col > max_amount,
                    )
                )
            else:
                self.query = self.query.filter(
                    or_(
                        and_(deposit_col >= min_amount, deposit_col <= max_amount),
                        and_(
                            withdrawal_col >= min_amount, withdrawal_col <= max_amount
                        ),
                    )
                )
        elif min_amount is not None:
            if invert:
                self.query = self.query.filter(
                    or_(deposit_col < min_amount, withdrawal_col < min_amount)
                )
            else:
                self.query = self.query.filter(
                    or_(deposit_col >= min_amount, withdrawal_col >= min_amount)
                )
        elif max_amount is not None:
            if invert:
                self.query = self.query.filter(
                    or_(deposit_col > max_amount, withdrawal_col > max_amount)
                )
            else:
                self.query = self.query.filter(
                    or_(deposit_col <= max_amount, withdrawal_col <= max_amount)
                )

        return self

    def _order_by(self, field: str, ascending: bool = True):
        if field == "date":
            if ascending:
                self.query = self.query.order_by(self.transaction_type.date)
            else:
                self.query = self.query.order_by(self.transaction_type.date.desc())
        elif field == "amount":
            if self.transaction_type == DebitTransaction:
                amount = func.coalesce(DebitTransaction.deposit, 0) - func.coalesce(
                    DebitTransaction.withdrawal, 0
                )
            elif self.transaction_type == CreditTransaction:
                amount = func.coalesce(CreditTransaction.charge, 0) - func.coalesce(
                    CreditTransaction.payment, 0
                )
            if ascending:
                self.query = self.query.order_by(func.abs(amount))
            else:
                self.query = self.query.order_by(func.abs(amount).desc())

        elif field == "description":
            if ascending:
                self.query = self.query.order_by(self.transaction_type.description)
            else:
                self.query = self.query.order_by(
                    self.transaction_type.description.desc()
                )
        return self

    def transactions(
        self,
        as_list: bool = False,
        order_by_amount: bool = False,
        order_by_description: bool = False,
        ascending: bool = True,
    ) -> list[DebitTransaction | CreditTransaction] | pd.DataFrame:
        # raise error if more than one order_by is True
        if sum([order_by_amount, order_by_description]) > 1:
            raise ValueError("Only one order_by argument can be True.")
        if order_by_amount:
            self._order_by("amount", ascending)
        elif order_by_description:
            self._order_by("description", ascending)
        else:
            self._order_by("date", ascending)

        if as_list:
            return self.query.all()
        else:
            self.transactions_list = self.query.order_by(
                self.transaction_type.date
            ).all()
            transactions_dicts = [to_dict(txn) for txn in self.transactions_list]
            df = pd.DataFrame(transactions_dicts)
            return df

    def count(
        self,
        order_by_count: bool = False,
        ascending: bool = True,
    ) -> int | pd.DataFrame:
        if self.group_by_attr:
            period = self._group_by_period()
            result = (
                self.query.with_entities(
                    period.label("period"), func.count().label("count")
                )
                .group_by(period)
                .all()
            )
            df = pd.DataFrame(result, columns=["period", "count"])
            if order_by_count:
                df = df.sort_values(by="count", ascending=ascending)
                # reset index to start from 0
                df.reset_index(drop=True, inplace=True)
            else:
                df = df.sort_values(by="period", ascending=ascending)
                # reset index to start from 0
                df.reset_index(drop=True, inplace=True)
            return df
        return self.query.count()

    def sum(
        self,
        order_by_date: bool = False,
        order_by_sum: bool = False,
        ascending: bool = True,
    ) -> float | pd.DataFrame:
        if self.group_by_attr:
            period = self._group_by_period()
            if self.transaction_type == DebitTransaction:
                result = (
                    self.query.with_entities(
                        period.label("period"),
                        func.coalesce(func.sum(DebitTransaction.deposit), 0).label(
                            "deposit_sum"
                        ),
                        func.coalesce(func.sum(DebitTransaction.withdrawal), 0).label(
                            "withdrawal_sum"
                        ),
                    )
                    .group_by(period)
                    .all()
                )
                df = pd.DataFrame(
                    result, columns=["period", "deposit_sum", "withdrawal_sum"]
                )
                df["sum"] = df["deposit_sum"] - df["withdrawal_sum"]
                df.drop(columns=["deposit_sum", "withdrawal_sum"], inplace=True)
            elif self.transaction_type == CreditTransaction:
                result = (
                    self.query.with_entities(
                        period.label("period"),
                        func.coalesce(func.sum(CreditTransaction.charge), 0).label(
                            "charge_sum"
                        ),
                        func.coalesce(func.sum(CreditTransaction.payment), 0).label(
                            "payment_sum"
                        ),
                    )
                    .group_by(period)
                    .all()
                )
                df = pd.DataFrame(
                    result, columns=["period", "charge_sum", "payment_sum"]
                )
                df["sum"] = df["charge_sum"] - df["payment_sum"]
                df.drop(columns=["charge_sum", "payment_sum"], inplace=True)

            if order_by_sum:
                df.sort_values(by="sum", ascending=ascending, inplace=True)
            else:
                df.sort_values(by="period", ascending=ascending, inplace=True)

            df.reset_index(drop=True, inplace=True)
            return df

        if self.transaction_type == DebitTransaction:
            deposit, withdrawal = self.query.with_entities(
                func.coalesce(func.sum(DebitTransaction.deposit), 0),
                func.coalesce(func.sum(DebitTransaction.withdrawal), 0),
            ).one()
            total = round((deposit or 0) - (withdrawal or 0), 2)
        elif self.transaction_type == CreditTransaction:
            charge, payment = self.query.with_entities(
                func.coalesce(func.sum(CreditTransaction.charge), 0),
                func.coalesce(func.sum(CreditTransaction.payment), 0),
            ).one()
            total = round((charge or 0) - (payment or 0), 2)

        return total

    def average(self) -> float | pd.DataFrame:
        if self.group_by_attr:
            period = self._group_by_period()
            result = (
                self.query.with_entities(
                    period.label("period"),
                    func.avg(
                        func.coalesce(self.transaction_type.deposit, 0)
                        - func.coalesce(self.transaction_type.withdrawal, 0)
                    ).label("average"),
                )
                .group_by(period)
                .all()
            )
            df = pd.DataFrame(result, columns=["period", "average"])
            return df
        total = self.sum()
        count = self.count()
        if count == 0:
            raise ValueError("No transactions found for the specified criteria.")
        return total / count

    def group_by(self, period: str):
        if period not in {"day", "week", "month", "year"}:
            raise ValueError(
                "Invalid period. Must be one of 'day', 'week', 'month', 'year'."
            )
        self.group_by_attr = period
        return self

    def _group_by_period(self):
        if self.group_by_attr == "day":
            return func.date(self.transaction_type.date)
        elif self.group_by_attr == "week":
            return func.strftime("%Y-%W", self.transaction_type.date)
        elif self.group_by_attr == "month":
            return func.strftime("%Y-%m", self.transaction_type.date)
        elif self.group_by_attr == "year":
            return func.strftime("%Y", self.transaction_type.date)
        else:
            return None

    def get_transactions_with_no_category(self):
        categories = CategoryManager().get_all_categories()

        for cat in categories:
            cat = category(cat)
            self.filter_description(description_contains=cat, invert=True)
        return self.transactions()


def category(name: str):
    cat = CategoryManager()
    return cat.get_keywords(name)


def to_dict(obj):
    # Convert an SQLAlchemy object to a dictionary
    # Exclude the id column
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns if c.key != "id"}
