---
title: "Local package install (required)"
last_modified: "2023-05-06"
---
> Last modified: {{ page.last_modified | date: "%Y-%m-%d"}}

The entire pipeline makes use of the local `top_fibers_pkg` Python package.
If you correctly initialize your environment (see [Setting up the conda environment](./environment.md)) then you should automatically have access to this package.

If for some reason this does not work, you can install the package locally yourself in the following way.
1. Change your current working directory to the `package` directory
    ```
    # Assumes you are on `Lenny`
    cd /home/data/apps/topfibers/repo/package
    ```
2. Use the pip local install 
    ```bash
    pip install -e .
    ```