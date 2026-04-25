import json
import pandas as pd
from tableau_api_lib import TableauServerConnection
import datetime
import os
import shutil
from pathlib import Path
import tableauserverclient as TSC
import subprocess
from dotenv import load_dotenv
load_dotenv()

time_now = datetime.datetime.now()

config = {
    "tableau_prod": {
        "server": os.environ.get("TABLEAU_SERVER"),
        "api_version": os.environ.get("TABLEAU_API_VERSION"),
        "personal_access_token_name": os.environ.get("TABLEAU_PAT_NAME"),
        "personal_access_token_secret": os.environ.get("TABLEAU_PAT_SECRET"),
        "site_name": os.environ.get("TABLEAU_SITE"),
        "site_url": os.environ.get("TABLEAU_SITE_URL"),
    }
}