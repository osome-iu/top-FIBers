# `database-functions/`
This directory contains all of the database functions. Each function maps to one
of the database tables.

- See the database schema [here](https://docs.google.com/spreadsheets/d/1gfAyLKk0VwQK1KLDgAWemWxw4wMM7_pyLtkgzAFehko/edit?usp=sharing)
- Generate the database with the `repo/data-loader/database_script/create.sql` script

### Contents
- `fib_indices.py`: contains functions to add data to **fib_indices** table
- `posts.py`: contains functions to add the data to **posts** table
- `reports.py`: contains functions to add the data to **reports** table
- `reshares.py`: contains functions to add the data to **reshares** table
- `profile_links.py` : contains functions to add and retrieve data from the **profile_links** table
