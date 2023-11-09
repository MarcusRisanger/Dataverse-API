import os

from msal import ConfidentialClientApplication
from msal_requests_auth.auth import ClientCredentialAuth
from sqlalchemy import create_engine, text

app_id = os.environ["app_id"]
authority_url = os.environ["authority_url"]
client_secret = os.environ["client_secret"]
url = os.environ["url"]  # https://org_identifier.crm.dynamics.com
scopes = [url + "/.default"]


client = ConfidentialClientApplication(
    client_id=app_id,
    client_credential=client_secret,
    authority=authority_url,
)
auth = ClientCredentialAuth(
    client=client,
    scopes=scopes,
)

DB = "org2ae976c6"
SERVER = "org2ae976c6.crm4.dynamics.com"


CONNSTRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    + f"SERVER={SERVER};DATABASE={DB};"
    + f"Authentication=ActiveDirectoryServicePrincipal;UID={app_id};"
    + f"PWD={client_secret};ACCESS_TOKEN={auth._get_access_token()}"
)

a = create_engine(f"mssql+pyodbc:///?odbc_connect={CONNSTRING}")


query = "SELECT vgr_altkey, vgr_rushmoreid FROM vgr_rushmorewell"
with a.connect() as c:
    result = c.execute(text(query)).fetchall()


# or using pandas

# import pandas as pd

# pd.read_sql_query(query, a)
