# `dataverse-api`

[![Build Status](https://github.com/MarcusRisanger/dataverse-api/workflows/release/badge.svg)](https://github.com/MarcusRisanger/dataverse-api/actions)
[![codecov](https://codecov.io/gh/MarcusRisanger/Dataverse-API/branch/main/graph/badge.svg)](https://codecov.io/gh/MarcusRisanger/Dataverse-API)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

The `dataverse-api` package is an abstraction layer developed for allowing simple interaction with Microsoft Dataverse Web API.

# Overview

The main goal of this project was to allow for simple upserts and inserts of data into Dataverse tables using simple and ubiquitous data structures, with use of batch requests to avoid frequent hits on the REST API. It is based on Python 3.9 to be compatible with current Python runtimes in Azure Functions.

### Getting started

Usage is fairly simple and assumes that a valid app registration for writing to Dataverse exists:

```
import os
from dataverse_api import DataverseClient

app_id = os.environ["app_id"]
authority_url = os.environ["authority_url"]
client_secret = os.environ["client_secret"]
url = os.environ["url"]
scopes = [url + "/.default"]


client = DataverseClient(
    app_id=app_id,
    client_secret=client_secret,
    authority_url=authority_url,
    scopes=scopes,
    dynamics_url=url,
)

table = client.entity(logical_name="xyz_my_table")

data = [
    {"xyz_my_table_key": "Foo", "xyz_my_table_col": 1010},
    {"xyz_my_table_key": "Bar", "xyz_my_table_col": 1020},
]

table.upsert(data)
```

### Optional validation

Instantiating a new `DataverseEntity` with `logical_name` triggers additional validation to take place, based on the EntityMetadata API endpoints. Upon instantiation, calls will be made to the API to fetch the `EntitySetName` used in API
queries, together with column names and alternate key Attribute combinations.

When validation is enabled, the client both checks that columns referred to in the data are valid according to the schema, and will automatically pick a suitable row ID for batch operations. While this is nice, it is mostly thought of as a debugging tool to develop scripts, since it carries the overhead of retrieving the information from the API. It is recommended to instantiate Entities by using the `entity_set_name` argument and specifying key columns in your data when preparing scripts for production.

## Development environment

We use [poetry](https://python-poetry.org) to manage dependencies and to administrate virtual environments. To develop
`dataverse-api`, follow the following steps to set up your local environment:

1.  [Install poetry](https://python-poetry.org/docs/#installation) if you haven't already.

2.  Clone repository:
    ```
    $ git clone git@github.com:MarcusRisanger/dataverse-api.git
    ```
3.  Move into the newly created local repository:
    ```
    $ cd dataverse-api
    ```
4.  Create virtual environment and install dependencies:
    ```
    $ poetry install
    ```

### Code requirements

All code must pass [black](https://github.com/ambv/black) and [isort](https://github.com/timothycrosley/isort) style
checks to be merged. It is recommended to install pre-commit hooks to ensure this locally before commiting code:

```
$ poetry run pre-commit install
```

Each public method, class and module should have docstrings. Docstrings are written in the [Google
style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).

### Testing

To produce Coverage tests, run the following commands

```
$ poetry run coverage run -m pytest
$ poetry run coverage xml
```
