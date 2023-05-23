"""
Purpose:
    Contains functions used to add data to REPORTS table.

Inputs:
    None

Outputs:
    None

Authors: Pasan Kamburugamuwa & Matthew DeVerna
"""
from flask import Flask
from library import backend_util

app = Flask(__name__)


def add_reports(date, report_name, platform):
    """
    Add data to the reports database table.

    Parameters
    -----------
    - date (str): the date of the report. Will be the date (format: "%Y-%m-%d")
        for which the report is generated.
    - report_name (str): the name of the report. Will be the date (format: ""%Y_%m"")
        for which the report is generated.
    - platform (str): the name of the platform (should be "twitter" or "facebook")

    Returns
    -----------
    result (dict): {"id" : add_report}
    """
    with backend_util.get_db_cursor() as cur:
        try:
            add_report = (
                "INSERT INTO reports "
                "(date, name, platform) "
                "values (%s, %s, %s) "
                "RETURNING id"
            )
            cur.execute(add_report, (date, report_name, platform))
            if cur.rowcount > 0:
                add_report = cur.fetchone()[0]
                result = {"id": add_report}
            return result
        except Exception as ex:
            raise Exception(ex)


def report_already_added(date, report_name, platform):
    """
    Check if a specific report already exists in the database

    Parameters
    -----------
    - date (str): the date of the report. Will be the date (format: "%Y-%m-%d")
        for which the report is generated.
    - report_name (str): the name of the report. Will be the date (format: ""%Y_%m"")
        for which the report is generated.
    - platform (str): the name of the platform (should be "twitter" or "facebook")

    Returns
    -----------
    result (bool): False if report exists. True if it does not.
    """
    with backend_util.get_db_cursor() as cur:
        select_query = (
            "SELECT id from reports where date= %s and name = %s and platform = %s;"
        )
        cur.execute(select_query, (date, report_name, platform))
        if cur.rowcount > 0:
            result = True
        else:
            result = False
        return result
