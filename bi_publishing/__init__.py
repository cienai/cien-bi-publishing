import time
import requests
import json
import msal
import urllib.parse

POWERBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"


def get_auth_token(config):
    app = msal.ConfidentialClientApplication(
        config["client_id"], authority=config["authority"],
        client_credential=config["secret"])

    result = None
    result = app.acquire_token_silent(config["scope"], account=None)

    if not result:
        print("No suitable token exists in cache. Let's get a new one from Azure AD.")
        result = app.acquire_token_for_client(scopes=config["scope"])
    # print(result)
    if "access_token" in result:
        access_token = result['access_token']
        return access_token
    else:
        print(result.get("error"))
        print(result.get("error_description"))
        print(result.get("correlation_id"))
        raise Exception("Failed to acquire token", result)


def _get_headers(client):
    return {
        "Authorization": f"Bearer {client['auth_token']}",
        "Content-Type": "application/json"
    }


def create_group(client, group_name):
    try:
        _ = get_group_by_name(client, group_name)
        print(f"Group={group_name} already exists")
        return
    except:  # noqa
        print(f"Group={group_name} not found, creating new group")
        api_url = f"{POWERBI_BASE_URL}/groups"
        body = {"name": group_name}
        response = requests.post(api_url, headers=_get_headers(client), data=json.dumps(body))
        if response.ok:
            print("--- created workspace ---")
        else:
            raise Exception(f"--- create workspace failed: {response.content} ---")


def get_groups(client):
    """
    returns a list of groups available to the client
    """
    api_url = f"{POWERBI_BASE_URL}/groups"
    response = requests.get(api_url, headers=_get_headers(client))
    if response.status_code == 200:
        groups = response.json()
        return groups['value']
    else:
        raise Exception(response.content)


def get_group_by_name(client, group_name):
    """
    return the group object for the given group name
    """
    for group in get_groups(client):
        if group['name'] == group_name:
            return group
    raise Exception(f"Group={group_name} not found")


def get_datasets_in_group(client, group_id):
    """
    returns a list of datasets in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/datasets"
    response = requests.get(api_url, headers=_get_headers(client))
    if response.status_code == 200:
        datasets = response.json()
        return datasets['value']
    else:
        raise Exception(response.content)


def get_dataset_by_name(client, group_id, dataset_name, retries=0, interval=1):
    """
    returns the dataset object for the given dataset name in the given group
    """
    for i in range(retries + 1):
        datasets = get_datasets_in_group(client, group_id)
        for ds in datasets:
            if ds['name'] == dataset_name:
                return ds
        print(f"==== request failed sleeping {interval}s ====")
        time.sleep(interval)
    raise ValueError(f"dataset '{dataset_name}' not found in group {group_id}")


def get_reports_in_group(client, group_id):
    """
    returns a list of reports in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/reports"
    response = requests.get(api_url, headers=_get_headers(client))
    if response.status_code == 200:
        reports = response.json()
        return reports['value']
    else:
        raise Exception(response.content)


def get_report_by_name(client, group_id, report_name, retries=0, interval=1):
    """
    returns the report object for the given report name in the given group
    """
    for i in range(retries + 1):
        reports = get_reports_in_group(client, group_id)
        for report in reports:
            if report['name'] == report_name:
                return report
        print(f"==== report not found. sleeping {interval}s ====")
        time.sleep(interval)

    raise ValueError(f"'{report_name}' not found in '{group_id}'")


def get_dashboards_in_group(client, group_id):
    """
    returns a list of dashboards in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/dashboards/"
    response = requests.get(api_url, headers=_get_headers(client))
    if response.status_code == 200:
        dashboards = response.json()
        return dashboards['value']
    else:
        raise Exception(response.content)


def upload_report_group(client, group, remote_report_name, local_pbix_file_path):
    """
    upload the given local pbix report file into the workspace(group)
    """
    headers = {
        "Authorization": f"Bearer {client['auth_token']}",
        "Content-Type": "multipart/form-data"
    }
    import_url = f"{POWERBI_BASE_URL}/groups/{group['id']}/imports?datasetDisplayName={remote_report_name}&nameConflict=Abort"
    file_name = "GTM Suite - Automatic Data Enhancement Report.pbix"
    files = {
        'file': (file_name, open(local_pbix_file_path, 'rb'), 'application/octet-stream')
    }
    response = requests.post(import_url, headers=headers, files=files)
    if response.ok:
        print("--- upload report complete ---")
    else:
        raise Exception(f"Upload failed: {response.content}")


def upload_datasest_to_group(client, group_id, remote_dataset_name, local_pbix_file_path):
    """
    upload the given local pbix dataset file into the powerbi service account workspace(group_id)
    """
    headers = {
        "Authorization": f"Bearer {client['auth_token']}",
        "Content-Type": "multipart/form-data"
    }
    import_url = f"{POWERBI_BASE_URL}/groups/{group_id}/imports?datasetDisplayName={remote_dataset_name}&skipReport=true"
    file_name = local_pbix_file_path.split("/")[-1] if '/' in local_pbix_file_path else local_pbix_file_path
    files = {
        'file': (file_name, open(local_pbix_file_path, 'rb'), 'application/octet-stream')
    }
    response = requests.post(import_url, headers=headers, files=files)
    if response.ok:
        return response.json()
    else:
        raise Exception("upload failed: ", response.content)


def rebind_report_to_dataset_in_group(client, report_id, group_id, dataset_id):
    """
    rebind the given report to the given dataset in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/reports/{report_id}/Rebind"
    print(f"--- rebind report: {report_id} to dataset: {dataset_id} ---")
    print(f"--- rebind url: {api_url} ---")
    body = {'datasetId': dataset_id}
    response = requests.post(api_url, headers=_get_headers(client), data=json.dumps(body))
    if response.ok:
        print("--- rebind successful ---")
    else:
        raise Exception(f"--- rebind failed: {response.content} ---")


def refresh_dataset_in_group(client, group_id, datasetId):
    """
    refresh the dataset in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/datasets/{datasetId}/refreshes"
    payload = {
        # "notifyOption": "MailOnFailure",
        # "retryCount": 3
    }
    payload_json = json.dumps(payload)
    response = requests.post(api_url, headers=_get_headers(client), data=payload_json)
    if response.status_code == 202:
        print("--- Dataset refresh request accepted. ---")
    else:
        print("Failed to refresh dataset. Status code:", response.status_code)
        print("Response:", response.text)
        raise Exception(response.text)


def delete_dataset_in_group(client, group_id, dataset_id):
    """
    delete the dataset in the given group
    """
    delete_url = f"{POWERBI_BASE_URL}/groups/{group_id}/datasets/{dataset_id}"
    headers = _get_headers(client)
    del headers['Content-Type']
    response = requests.delete(delete_url, headers=headers)
    if response.ok:
        print("delete successful")
    else:
        raise ValueError(f"Failed to delete. result= {response.content}")


def delete_dashboard_in_group(client, group_id, dashboard_id):
    """
    delete the dashboard in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/dashboards/{dashboard_id}"
    headers = _get_headers(client)
    del headers['Content-Type']
    response = requests.delete(api_url, headers=headers)
    if response.ok:
        print("delete successful")
    else:
        raise ValueError(f"Failed to delete. result= {response.content}")


def delete_report_in_group(client, group_id, report_id):
    """
    delete the report in the given group
    """
    delete_url = f"{POWERBI_BASE_URL}/groups/{group_id}/reports/{report_id}"
    headers = _get_headers(client)
    del headers['Content-Type']
    response = requests.delete(delete_url, headers=headers)
    if response.ok:
        print("delete successful")
    else:
        raise ValueError(f"Failed to delete. result= {response.content}")


def remove_everything_in_group(client, group_id, prefix):
    """
    delete all reports, datasets and dashboards in the given group that start with the given prefix
    """
    # datasets are semantic models
    datasets = get_datasets_in_group(client, group_id)
    print("number of datasets found: ", len(datasets))
    for dataset in datasets:
        if dataset['name'].startswith(prefix):
            print('-' * 100)
            print(f"deleting dataset: {dataset['name']}")
            delete_dataset_in_group(client, group_id, dataset['id'])

    # dashboards are dummy reports that get created when you upload a semantic model
    dashboards = get_dashboards_in_group(client, group_id)
    print("number of dashboards found: ", len(dashboards))
    for dashboard in dashboards:
        if dashboard['name'].startswith(prefix):
            print('-' * 100)
            print(f"deleting dashboard: {dashboard['name']}")
            delete_dashboard_in_group(client, group_id, dashboard['id'])

    # reports are the final report UI that we use/present
    reports = get_reports_in_group(client, group_id)
    print("number of reports found: ", len(reports))
    for report in reports:
        if report['name'].startswith(prefix):
            print('-' * 100)
            print(f"deleting report: {report['name']}")
            delete_report_in_group(client, group_id, report['id'])


def clone_report_in_group(client, source_group_id, target_group_id, report_name, report_id, target_dataset_id):
    """
    clone the given report in the source group to the target group
    """
    clone_url = f"{POWERBI_BASE_URL}/groups/{source_group_id}/reports/{report_id}/Clone"
    export_headers = _get_headers(client)
    data = {
        "name": report_name,
        "targetWorkspaceId": target_group_id,
        "targetModelId": target_dataset_id
    }
    export_response = requests.post(clone_url, headers=export_headers, data=json.dumps(data))
    if export_response.ok:
        return export_response.json()
    raise Exception("Clone report failed: ", export_response.content)


def update_dataset_params(client, db_name, dw_conn, group_id, dataset_id):
    """
    update the dataset parameters in the given group
    """
    db_type = "INVALID"
    if dw_conn['type'] == 'postgres':
        db_type = "PostgresSQL"
    elif dw_conn['type'] == 'mssql':
        db_type = "Sql"

    if db_type == "INVALID":
        raise ValueError("Invalid database type")

    postgres_host = dw_conn['host'] if db_type == "PostgresSQL" else "INVALID_HOST"  # doesn't work if ''
    sql_server_host = dw_conn['host'] if db_type == "Sql" else "INVALID_HOST"  # doesn't work if ''
    details = {
        "updateDetails": [
            {"name": 'db_name', "newValue": db_name},
            {"name": 'db_server_postgres', "newValue": postgres_host},
            {"name": 'db_server_sql', "newValue": sql_server_host},
            {"name": 'db_type', "newValue": db_type}
        ]
    }

    update_params_url = f"{POWERBI_BASE_URL}/groups/{group_id}/datasets/{dataset_id}/Default.UpdateParameters"
    res = requests.post(update_params_url, headers=_get_headers(client), data=json.dumps(details))
    if res.ok:
        print("--- params updated ---")
    else:
        raise Exception("Failed to update params: ", res.content)


def update_dataset_credentials(client, dw_conn, group_id, dataset_id):
    """
    update the dataset credentials in the given group
    """
    url = f"{POWERBI_BASE_URL}/groups/{group_id}/datasets/{dataset_id}/datasources"
    print('--- getting datasources for: ', url)
    res = requests.get(url, headers=_get_headers(client))
    datasources = res.json()['value']
    username = dw_conn['username']
    password = dw_conn['password']

    for datasource in datasources:
        if datasource['datasourceType'] in ['PostgreSql', 'Sql']:
            credentials_update = {
                "credentialDetails": {
                    "credentialType": 'Basic',
                    "credentials": json.dumps({"credentialData": [{"name": "username", "value": username}, {"name": "password", "value": password}]}),
                    "encryptedConnection": 'Encrypted',
                    "encryptionAlgorithm": "None",
                    "privacyLevel": "Organizational",
                    "useEndUserOAuth2Credentials": "False"
                }
            }
            url = f"{POWERBI_BASE_URL}/gateways/{datasource['gatewayId']}/datasources/{datasource['datasourceId']}"
            res = requests.patch(url, headers=_get_headers(client), data=json.dumps(credentials_update))
            if res.ok:
                print("--- credentials updated successfully ---")
            else:
                raise Exception("Failed to update credentials: ", res.content)
        else:
            print("======= UNKOWN DATASOURCE FOUND =========", datasource)


def get_users_in_group(client, group_id):
    """
    returns a list of users in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/users"
    headers = _get_headers(client)
    response = requests.get(api_url, headers=headers)
    if response.ok:
        return response.json()['value']
    else:
        raise Exception(f"--- failed to get users: {response.content} ---")


def add_user_to_group(client, group_id, email_id, user_type):
    """
    add the given user to the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/users"
    headers = _get_headers(client)
    data = {
        "emailAddress": email_id,
        "groupUserAccessRight": user_type  # "Admin"
    }

    response = requests.post(api_url, headers=headers, data=json.dumps(data))
    if response.ok:
        print(f'--- {email_id} added to group {group_id} as {user_type}')
    else:
        raise Exception(f"--- failed to add user {email_id} {response.content} ---")


def get_client(pbi_workspace_conn, scope_overrides=None):
    """
    returns a client object that can be used to interact with the PowerBI service
    """
    config = _get_config(pbi_workspace_conn, scope_overrides)
    print(f'config: {config}')
    print(f'scopes: {config["scope"]}')
    token = get_auth_token(config)
    client = {
        'auth_token': token,
    }
    return client


def _get_config(pbi_workspace_conn, scope_overrides=None):
    """
    helper function that sets parameters used to get the client
    """
    config = {}
    config["secret"] = pbi_workspace_conn['CLIENT_SECRET']
    config["client_id"] = pbi_workspace_conn['CLIENT_ID']
    config["authority"] = f"https://login.microsoftonline.com/{pbi_workspace_conn['TENANT_ID']}"

    scopes = ["https://analysis.windows.net/powerbi/api/.default"]
    if scope_overrides:
        config["scope"] = scope_overrides
    else:
        config["scope"] = scopes

    return config


def download_file_from_integration_hub(filename, local_file_name):
    print(f"--- downloading: {filename}")

    encoded_file = urllib.parse.quote(filename)
    url = "https://github.com/cienai/IntegrationHub/raw/main/powerbi/" + encoded_file
    r = requests.get(url, allow_redirects=True)
    open(local_file_name, 'wb').write(r.content)


def get_capcities(client):
    api_url = f"{POWERBI_BASE_URL}/capacities"
    response = requests.get(api_url, headers=_get_headers(client))
    if response.status_code == 200:
        capacities = response.json()
        return capacities['value']
    else:
        raise Exception(response.content)


def get_capacity_by_name(client, capacity_name):
    capacities = get_capcities(client)
    for capacity in capacities:
        if capacity['displayName'] == capacity_name:
            return capacity
    raise ValueError(f"capacity: {capacity_name} not found")


def add_group_to_capacity(client, group_id, capacity_id):
    """
    Add the given group/workspace to the given capacity
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/AssignToCapacity"
    print(f"--- adding group: {group_id} to dataset: {capacity_id} ---")
    print(f"--- add url: {api_url} ---")
    body = {'capacityId': capacity_id}
    response = requests.post(api_url, headers=_get_headers(client), data=json.dumps(body))
    if response.ok:
        print("--- add successful ---")
    else:
        raise Exception(f"--- add failed: {response.content} ---")
