# `dataverse-api`

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Build Status](https://github.com/MarcusRisanger/dataverse-api/workflows/release/badge.svg)](https://github.com/MarcusRisanger/dataverse-api/actions)
[![codecov](https://codecov.io/gh/MarcusRisanger/Dataverse-API/branch/main/graph/badge.svg)](https://codecov.io/gh/MarcusRisanger/Dataverse-API)

The `dataverse-api` package is an abstraction layer developed for allowing simple interaction with Microsoft Dataverse Web API.

# Table of Contents
* [Description](#Description)
* [Getting Started](#getting-started)
* [Development environment](#development-environment)
    * [Code requirements](#code-requirements)
    * [Testing](#testing)
* [To do](#to-do)
* [Usage](#usage)
    * [DataverseClient](#dataverseclient)
        * [Initialize DataverseClient](#initialize-dataverseclient)
        * [Create new Entity](#create-new-entity)
        * [Update existing Entity](#update-existing-entity)
    * [DataverseEntity](#dataverseentity)
        * [Initialize Entity interface](#initialize-interface-with-entity)
        * [Read](#read)
        * [Create](#create)
        * [Upsert](#upsert)
        * [Delete](#delete)
        * [Add and remove Attributes](#add-and-remove-attributes)
        * [Add and remove Alternate Keys](#add-and-remove-alternate-keys)



# Description

The main goal of this project was to enable some use-cases against Dataverse that I wanted to explore for a work assignment, while getting some experience in programming and testing out different ways of setting up the codebase.

The functionality I have built into this Dataverse wrapper constitutes the functionality I have wanted to explore myself.

Most important is to enable creating, upserting, updating and deleting rows of data into Dataverse tables using common data structures, and implementing choices on how these requests are to be formed. For example, when creating new rows, the user can choose between individual `POST` requests per row, combining data into batch requests against the `$batch` endpoint, or even to use the `CreateMultiple` Dataverse action.

The framework is written in Python 3.11, seeing as this runtime is available in the current release of Azure Functions.

## Getting started

Usage is fairly simple - authentication must be handled by the user. The `DataverseClient` simply accepts an already authorized `requests.Session` with which to handle API requests.

I suggest using `msal` and `msal-requests-auth` for authenticating the `Session`. The examples below include this way of implementing auth:

```python
import os

from msal import ConfidentialClientApplication
from msal_requests_auth.auth import ClientCredentialAuth
from requests import Session

from dataverse_api import DataverseClient

# Prepare Auth
app_id = os.getenv("app_id")
secret = os.getenv("client_secret")
environment_url = os.getenv("environment_url")
authority_url = os.getenv("authority_url")
app_reg = ConfidentialClientApplication(
    client_id=app_id,
    client_credential=secret,
    authority=authority_url,
)
auth = ClientCredentialAuth(
    client=app_reg,
    scopes=[environment_url + "/.default"]
)

# Prepare Session
session = Session()
session.auth = auth

# Instantiate DataverseClient
client = DataverseClient(session, environment_url)

# Instantiate interface to Entity
entity = client.entity(logical_name="organization")

# Read data!
entity.read(select=["name"])
```


## Development environment

[poetry](https://python-poetry.org) is used for managing dependencies. To develop
`dataverse-api`, follow the below steps to set up your local environment:

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

All code must pass [ruff](https://github.com/astral-sh/ruff) style checks to be merged. It is recommended to install pre-commit hooks to ensure this locally before commiting code:

```
$ poetry run pre-commit install
```

Each public method, class and module should have docstrings. Docstrings are written in the Numpy style.

### Testing

To produce Coverage reports, run the following commands:

```
$ poetry run coverage run -m pytest
$ poetry run coverage xml
```

## To Do:
* Documentation ..
* Metadata
    - Choice
    - Multichoice
    - Money
* Entity implementation
    - Illegal file extensions
    - Upload file
    - Upload image
    - Upload large file
    - Add / remove relationships
    - Add relationships (single/collection valued) as attr on entity
    - Add columns as attr on entity
* Add Tests:
    - Add column
    - Remove column
    - Add alternate key
    - Remove alternate key
* Implement `CreateMultiple` request if creating more than 100 elements?
    - https://learn.microsoft.com/en-us/power-apps/developer/data-platform/bulk-operations?tabs=webapi#createmultiple
* Implement BulkDelete Action?
    - https://learn.microsoft.com/en-us/power-apps/developer/data-platform/delete-data-bulk?tabs=sdk

# Usage

## DataverseClient

### Initialize DataverseClient

TBD

### Create new Entity

It is possible to create a new Entity using the `DataverseClient`. This requires a full `EntityMetadata` definition according to Dataverse standards. You can make this yourself and follow the `MetadataDumper` protocol, or use the provided `define_entity` function.

The `define_label` function makes it simple to generate `Label` metadata with correct `LocalizedLabels` in its payload.

In the example below, the optional `return_representation` argument has been set to `True` to receive the full Entity metadata definition as created by Dataverse as part of the server response. The response can be parsed by `EntityMetadata` classmethod to get a full fledged object for editing.

```python
from dataverse_api.metadata.attributes import StringAttributeMetadata
from dataverse_api.metadata.entity import EntityMetadata, define_entity
from dataverse_api.utils.labels import define_label

new_entity = define_entity(
    schema_name="new_name",
    attributes=[StringAttributeMetadata(
        schema_name="new_primary_col",
        is_primary_name = True,
        description=define_label("Primary column for Entity."),
        display_name=define_label("Autonumber Column"),
        auto_number_format="{SEQNUM:6}-#-{RANDSTRING:3}")],
    description=define_label("Entity Created by Client"),
    display_name=define_label("Programmatically Created Table")
)

resp = client.create_entity(new_entity, return_representation=True)
entity_meta = EntityMetadata.model_validate_dataverse(resp.json())
```

### Update existing Entity

You can update an existing Entity definition easily by retrieving the Entity metadata definition, and reupload an adjusted version.

Below is a simple example. Note that this method also supports `return_representation` in the same manner as the `DataverseClient.create_entity()` method, if you want to return the edited Entity metadata as persisted in Dataverse.

```python
metadata = client.get_entity_definition("new_name")
metadata.display_name.localized_labels[0].label = "Overridden Display Name"

client.update_entity(metadata)
```

## DataverseEntity

### Initialize interface with Entity

To initializes an interface with a specific Dataverse Entity, use the `DataverseClient.entity()` method. It returns a `DataverseEntity` object that allows interaction with this specific entity.

```python
entity = client.entity(logical_name="foo")
```
As of now, only `LogicalName` is supported for instantiating a new `DataverseEntity` object.


### Read

TBD

### Create

TBD

### Upsert

TBD

### Delete

TBD

### Add and remove Attributes

TBD

## Add and remove Alternate Keys

TBD
