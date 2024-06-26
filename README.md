# bi_publishing

## Overview

`bi_publishing` is a Python package designed to streamline the process of publishing PowerBI workspaces, reports and datasets.

## Installation

You can install `bi_publishing` via pip:

```
pip install git+https://github.com/cienai/cien-bi-publishing.git@main
```

## Usage

### 1. Importing the Package

First, import the `bi_publishing` package into your Python script:

```python
import bi_publishing
```

### 2. Creating the Client and Connection Strings

To create a client, you need to create a registered app within Azure.  Make note of the tenant_id, client_id and client_secret.

- https://learn.microsoft.com/en-us/power-bi/developer/embedded/register-app?tabs=customers
- https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app


Once you have a registered app, you will need to construct a properly formmated connection object which will be used to construct a client. The format is as follows:
```python
import bi_publishing
import time

# TODO: put your workspace name and database name here
workspace_name = "My Test Workspace"  # YOUR_WORKSPACE_NAME
db_name = "cien_zadfwcjid24030145_db"  # YOUR_DB_NAME

# create the connection definition
pbi_conn = {
    "TENANT_ID": "<your tenant id>",
    "CLIENT_ID": "<your client id>",
    "CLIENT_SECRET": "<your client secret>"
}

# define the data warehouse connection
dw_conn = {
    "type": "mssql", # "mssql" for sql server and "postgres" for postgres db
    "host": "<db host>",
    "username": "<db username>",
    "password": "<db password>",  
}


# this is the current datasets and their associated reports. KEEP THIS PART UP-TO-DATE or change as necessary
DATASET_REPORT_MAPPING = {
    "Dataset - SQL Server - Automatic Data Enhancement.pbix": ["Automatic Data Enhancement Report.pbix"],
    "Dataset - SQL Server - Sales Performance.pbix":  [
        "Management Report.pbix",
        "Strategy Report.pbix",
        "Tactical Report.pbix",
        "Utility Box Report.pbix"
    ]
}

# create the pbi client
client = bi_publishing.get_client(pbi_conn)

# create the workspace
bi_publishing.create_group(client, workspace_name)

# get back the target workspace where we will do all the publishing
target_group = bi_publishing.get_group_by_name(client, workspace_name)
print(f"--- {target_group} ---")

# ADMIN users to add to the workspace
admin_users = [
    # "admin1@test.com",
    # "admin2@test.com",
]

# MEMBER users to add to the workspace
member_users = [
    # "member1@test.com",
    # "member2@test.com",
]

users = bi_publishing.get_users_in_group(client, target_group['id'])
userids = [i['identifier'] for i in users]

for email_id in admin_users:
    if email_id in userids:
        print(f"--- user {email_id} already in group ---")
    else:
        bi_publishing.add_user_to_group(client, target_group['id'], email_id, "Admin")

for email_id in member_users:
    if email_id in userids:
        print(f"--- user {email_id} already in group ---")
    else:
        bi_publishing.add_user_to_group(client, target_group['id'], email_id, "Member")

prefix = ""
print("--- cleaning up target workspace ---")
bi_publishing.remove_everything_in_group(client, target_group['id'], prefix)
tag = 'v4.1.14'
for dset, reports in DATASET_REPORT_MAPPING.items():
    print(f"=============== processing dataset: {dset} ===============")
    # ==== 0. download the dataset
    bi_publishing.download_file_from_integration_hub(tag, dset, dset)
    # ensure that the dataset is disconnected
    bi_publishing.disconnect_pbix(dset)

    # ==== 1. upload dataset
    remote_daset_name = f"{prefix} {dset}".replace('.pbix', '')
    print(f'--- uploading dataset: {remote_daset_name}')
    bi_publishing.upload_datasest_to_group(client, target_group['id'], remote_daset_name, dset)

    # if you immediately query after uploading a dataset it doesn't show up, so sleep for few second interval with retries
    dataset = bi_publishing.get_dataset_by_name(client, target_group['id'], dataset_name=remote_daset_name, retries=5, interval=10)
    print(f"--- Uploaded dataset: {dataset['id']} ---")

    # ==== 2. Upload the reports
    # because we can directly import the dataset and reports, we don't need to clone the reports separately
    for report in reports:
        bi_publishing.download_file_from_integration_hub(tag, report, report)

        # to make the reports deploy across tenants
        bi_publishing.disconnect_pbix(report)
        bi_publishing.connect_pbix(report, target_group['id'], dataset['id'])

        report_name = f"{prefix} {report}".replace('.pbix', '')
        print(f"--- uploading report: {report_name}")
        bi_publishing.upload_report_group(client, target_group, report_name, report)
        # wait for the report to show up just like datasets it takes a few seconds
        time.sleep(2)
        rep_obj = bi_publishing.get_report_by_name(client, target_group['id'], report_name, retries=5, interval=10)

        # ==== 3. rebind the report to the dataset
        bi_publishing.rebind_report_to_dataset_in_group(client, rep_obj['id'], target_group['id'], dataset['id'])

    # ==== 4. update parameters in dataset
    bi_publishing.update_dataset_params(client, db_name, dw_conn, target_group['id'], dataset['id'])

    # ==== 5. update credentials in the dataset
    bi_publishing.update_dataset_credentials(client, dw_conn, target_group['id'], dataset['id'])

    # ==== 6. refresh the dataset
    print("--- refreshing dataset: ", dataset['id'])
    bi_publishing.refresh_dataset_in_group(client, target_group['id'], dataset['id'])
```


## Contributing

Contributions are welcome! If you encounter any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request on the GitHub repository.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
