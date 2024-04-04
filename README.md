# MidasTouch

MidasTouch is a simple package that allows you to parse and analyze bank account lists of transactions/statements. With MidasTouch, you can easily extract information from your bank account data and perform various analysis tasks.

## Current Features
- **Parsing**: MidasTouch can parse transaction data from CSV files downloaded from various banks.
- **Validation**: The package includes built-in functionality to validate transaction data, ensuring that it is accurate and complete. No need to worry about giving it the same data twice.
- **Saving**: MidasTouch automatically saves the parsed data (database format) to a hidden application data folder, so you can access it later without having to keep the original CSV files.
- **Analysis**: The package offers a range of analytical tools, such as sums, averages, and counts of transactions, as well as the ability to filter and sort transactions based on date, in/out, and keywords in the transaction description.
- **Account Types**: MidasTouch can handle multiple account types, such as debit and credit card accounts.

## Upcoming Features

- **Parsing**: Currently, only CSV files from TD Bank and Desjardins are supported. Future versions will support more banks and file formats.
- **Categorization**: MidasTouch will allow you to categorize transactions based on customizable rules, making it easier to analyze spending patterns and identify trends.
- **Analytics**: The package offers a range of analytical tools, such as generating summary statistics, visualizing spending patterns, and identifying outliers.
- **Account Merge**: MidasTouch will allow you to merge transactions from multiple accounts into a single database, making it easier to analyze your overall financial situation. For example, you can merge your debit and credit card transactions to get a complete picture of your spending habits with automatic removal of transfer transactions between accounts.
