import os
import re
from datetime import date, datetime

import pandas as pd
import pdftotext


def convert_td_statement_to_csv(file_name: str, year: int):
    with open(file_name, "rb") as file:
        pdf = pdftotext.PDF(file, physical=True)

    dataframes = []
    counter = 0
    for page in pdf:
        counter += 1
        print(f"Converting page {counter} out of {len(pdf)}")
        lines = page.split("\n")

        for i, line in enumerate(lines):
            if "Description" in line:
                lines = lines[i + 2 :]
                break

        for i, line in enumerate(lines):
            if line.strip() == "":
                # Find next non empty line
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() != "":
                        totals = lines[j]
                        break
                lines = lines[:i]
                break
            # if line contains only numbers, spaces, and commas, it is a total line
            elif re.match(r"^[0-9, ]+$", line.strip()):
                totals = line
                lines = lines[:i]
                break
        totals = totals.split("    ")
        totals = list(filter(None, totals))
        total_withdrawals = totals[0].strip().replace(",", ".").replace(" ", "")
        total_deposits = totals[1].strip().replace(",", ".").replace(" ", "")

        # make new list of the 30 first characters of each line (and clean up the lines)
        descriptions = [line[:40].strip() for line in lines]
        descriptions = [re.sub(" +", " ", line) for line in descriptions]
        # add space to the end to normalize length to 20
        descriptions = [line + " " * (20 - len(line)) for line in descriptions]

        # find start positions of columns
        start_withdrawals = 10000
        for line in lines:
            for i in range(40, len(line)):
                if line[i] != " ":
                    start_withdrawals = min(start_withdrawals, i)
                    break

        prev = start_withdrawals + 10
        start_deposits = 10000
        for line in lines:
            for i in range(prev, len(line)):
                if line[i] != " " and line[i - 1] == " " and line[i - 2] == " ":
                    start_deposits = min(start_deposits, i)
                    break

        prev = start_deposits + 10
        start_dates = 10000
        for line in lines:
            for i in range(prev, len(line)):
                if line[i] != " ":
                    start_dates = min(start_dates, i)
                    break

        prev = start_dates + 10
        start_balance = 10000
        for line in lines:
            for i in range(prev, len(line)):
                if line[i] != " ":
                    start_balance = min(start_balance, i)
                    break

        # makes a list of the rest of the lines

        withdrawals = [
            line[start_withdrawals - 1 : start_deposits - 5].strip() for line in lines
        ]

        deposits = [
            line[start_deposits - 1 : start_dates - 2].strip() for line in lines
        ]

        dates = [line[start_dates - 1 : start_balance - 5].strip() for line in lines]

        balance = [line[start_balance - 1 :].strip() for line in lines]

        # Print all lines by adding | at start_ positions defined above
        for i in range(len(lines)):
            print(
                f"{lines[i][:start_withdrawals-1]}|{lines[i][start_withdrawals:start_deposits-1]}|{lines[i][start_deposits:start_dates-1]}|{lines[i][start_dates:start_balance-1]}|{lines[i][start_balance:]}"
            )

        # Dates are formatted as, for example, 30JUL or 03JUN (month and day). Convert to datetime
        dates = [date.replace("JAN", "Jan") for date in dates]
        dates = [date.replace("F¯V", "Feb") for date in dates]
        dates = [date.replace("MAR", "Mar") for date in dates]
        dates = [date.replace("AVR", "Apr") for date in dates]
        dates = [date.replace("MAI", "May") for date in dates]
        dates = [date.replace("JUN", "Jun") for date in dates]
        dates = [date.replace("JUL", "Jul") for date in dates]
        dates = [date.replace("AO", "Aug") for date in dates]
        dates = [date.replace("SEP", "Sep") for date in dates]
        dates = [date.replace("OCT", "Oct") for date in dates]
        dates = [date.replace("NOV", "Nov") for date in dates]
        dates = [date.replace("DÉC", "Dec") for date in dates]
        dates = [date.replace("D¯C", "Dec") for date in dates]
        abbreviated_month_names = []
        for month in range(1, 13):
            date_obj = date(year=2021, month=month, day=1)
            abbreviated_month_names.append(date_obj.strftime("%b"))

        # turn dates into datetime objects
        for i in range(len(dates)):
            month_name = dates[i][2:5]
            month = abbreviated_month_names.index(month_name) + 1
            day = int(dates[i][:2])
            year = 2021
            date_obj = datetime(year=year, month=month, day=day)
            dates[i] = date_obj

        # Convert the lists to a DataFrame
        df = pd.DataFrame(
            {
                "Date": dates,
                "Description": descriptions,
                "Withdrawal": withdrawals,
                "Deposit": deposits,
                "Balance": balance,
            }
        )
        # Replace commas with periods and remove space (used to separate thousands in the PDF file)
        df["Withdrawal"] = df["Withdrawal"].str.replace(",", ".").str.replace(" ", "")
        df["Deposit"] = df["Deposit"].str.replace(",", ".").str.replace(" ", "")
        df["Balance"] = df["Balance"].str.replace(",", ".").str.replace(" ", "")

        # Convert the columns to the correct data types (if empty, write nan)
        df["Withdrawal"] = pd.to_numeric(df["Withdrawal"], errors="coerce")
        df["Deposit"] = pd.to_numeric(df["Deposit"], errors="coerce")
        df["Balance"] = pd.to_numeric(df["Balance"], errors="coerce")

        for i in range(1, len(df)):
            if pd.isnull(df.loc[i, "Balance"]):
                if pd.notnull(df.loc[i, "Withdrawal"]):
                    df.loc[i, "Balance"] = (
                        df.loc[i - 1, "Balance"] - df.loc[i, "Withdrawal"]  # type: ignore
                    )
                elif pd.notnull(df.loc[i, "Deposit"]):
                    df.loc[i, "Balance"] = df.loc[i - 1, "Balance"] + df.loc[i, "Deposit"]  # type: ignore
        for i in range(
            len(df) - 2, -1, -1
        ):  # Start from the second to last row and move upwards
            if pd.isnull(df.loc[i, "Balance"]):
                # Check if next row has a balance to calculate from
                if pd.notnull(df.loc[i + 1, "Balance"]):
                    if pd.notnull(df.loc[i + 1, "Withdrawal"]):
                        df.loc[i, "Balance"] = df.loc[i + 1, "Balance"] + df.loc[i + 1, "Withdrawal"]  # type: ignore
                    elif pd.notnull(df.loc[i + 1, "Deposit"]):
                        df.loc[i, "Balance"] = df.loc[i + 1, "Balance"] - df.loc[i + 1, "Deposit"]  # type: ignore

        # Round the balance to two decimals
        df["Balance"] = df["Balance"].round(2)

        # assert that total withdrawals and deposits are correct
        assert round(df["Withdrawal"].sum(), 2) == round(float(total_withdrawals), 2)
        assert round(df["Deposit"].sum(), 2) == round(float(total_deposits), 2)

        dataframes.append(df)

    # Concatenate the DataFrames
    total_df = pd.concat(dataframes, ignore_index=True)
    # Save the DataFrame to a CSV file, but remove the column titles
    total_df.to_csv(file_name[:-4] + ".csv", index=False, header=False)


if __name__ == "__main__":

    files = os.listdir("pdfs")
    files = [f"pdfs/{file}" for file in files if file.endswith(".pdf")]

    for file in files:
        print(f"Converting {file}")
        convert_td_statement_to_csv(file, int(file[-8:-4]))
