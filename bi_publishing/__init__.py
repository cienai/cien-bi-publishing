import base64
import time
import requests
import json
import msal
import urllib.parse
import zipfile
import os
import hashlib

POWERBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"
FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"


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


def set_group_to_large_semantic_model(client, group_id):
    url = f"{POWERBI_BASE_URL}/groups/{group_id}"
    res = requests.patch(url, headers=_get_headers(client), data=json.dumps({"defaultDatasetStorageFormat": "Large"}))
    if res.ok:
        print("--- workspace set to use large semantic models successfully ---")
    else:
        raise Exception("Failed to update workspace to large models: ", res.content)


def get_datasets_in_group(client, group_id, retries=0, interval=1):
    """
    returns a list of datasets in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/datasets"
    for i in range(retries + 1):
        response = requests.get(api_url, headers=_get_headers(client))
        if response.status_code == 200:
            datasets = response.json()
            return datasets['value']
        print(f"==== request failed sleeping {interval}s ====")
        time.sleep(interval)

    raise ValueError(response.content)


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


def takeover_dataset_in_group(client, group_id, dataset_id):
    api_url = F"{POWERBI_BASE_URL}/groups/{group_id}/datasets/{dataset_id}/Default.TakeOver"
    body = {}
    response = requests.post(api_url, headers=_get_headers(client), data=json.dumps(body))
    if response.ok:
        print("--- dataset taken over ---")
    else:
        raise Exception(f"--- dataset takeover failed: {response.content} ---")


def get_reports_in_group(client, group_id, retries=0, interval=1):
    """
    returns a list of reports in the given group
    """
    api_url = f"{POWERBI_BASE_URL}/groups/{group_id}/reports"
    for i in range(retries + 1):
        response = requests.get(api_url, headers=_get_headers(client))
        if response.status_code == 200:
            reports = response.json()
            return reports['value']
        print(f"==== request failed sleeping {interval}s ====")
        time.sleep(interval)

    raise ValueError(response.content)


def get_report_by_name(client, group_id, report_name, retries=0, interval=1):
    """
    returns the report object for the given report name in the given group
    """
    for i in range(retries + 1):
        # if the report is uploaded immediately before this step, it doesn't show up immediately. you'd have to wait and retry until it shows up.
        reports = get_reports_in_group(client, group_id, retries=5, interval=10)
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


def get_page_urls_for_report(client, group_id, report_id):
    pages_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}/pages"
    response = requests.get(pages_url, headers=_get_headers(client))
    pages = response.json().get('value', [])

    output = []
    for page in pages:
        page_id = page['name']
        display_name = page['displayName']
        url = f"https://app.powerbi.com/groups/{group_id}/reports/{report_id}/{page_id}?experience=power-bi"

        # You can replace this with a lookup or metadata if you have descriptions stored elsewhere
        description = "No description available"  # Placeholder
    
        output.append({
            "page_name": display_name,
            "page_id": page_id,
            "page_url": url,
            # "description": description
        })
    return output


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
        return response.json()
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


def update_report_content_in_group(client, group_id, src_report_id, target_report_id):
    api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{target_report_id}/UpdateReportContent"
    body = {
        "sourceReport": {
            "sourceReportId": src_report_id,
            "sourceWorkspaceId": group_id
          },
          "sourceType": "ExistingReport"
    }
    response = requests.post(api_url, headers=_get_headers(client), data=json.dumps(body))
    if response.ok:
        print("--- report content updated ---")
    else:
        raise Exception(f"--- report content update failed: {response.content} ---")


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
        db_type = "SQL Server"
    elif dw_conn['type'] == 'azure-datalake':
        db_type = "Azure Data Lake"

    if db_type == "INVALID":
        raise ValueError("Invalid database type")

    postgres_host = dw_conn['host'] if db_type == "PostgresSQL" else "INVALID_HOST"  # doesn't work if ''
    sql_server_host = dw_conn['host'] if db_type == "SQL Server" else "INVALID_HOST"  # doesn't work if ''
    details = {
        "updateDetails": [
            {"name": 'db_name', "newValue": db_name},
            {"name": 'db_server_postgres', "newValue": postgres_host},
            {"name": 'db_server_sql', "newValue": sql_server_host},
            {"name": 'db_type', "newValue": db_type}
        ]
    }

    if db_type == "Azure Data Lake":
        file_path = dw_conn['BUCKET_URI'].replace('blob', 'dfs').replace('wasbs://', '')
        file_server_data_lake = 'https://' + file_path.split('/')[0] + '/'
        file_folder_data_lake = '/'.join(file_path.split('/')[1:]) + '/export/'

        details['updateDetails'].append({"name": 'file_server_data_lake', "newValue": file_server_data_lake})
        details['updateDetails'].append({"name": 'file_folder_data_lake', "newValue": file_folder_data_lake})

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

    for datasource in datasources:
        if datasource['datasourceType'] in ['PostgreSql', 'Sql']:
            username = dw_conn['username']
            password = dw_conn['password']

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
        elif datasource['datasourceType'] == 'AzureDataLakeStorage':
            sas_token = dw_conn['AZURE_STORAGE_SAS_TOKEN']
            credentials_update = {
                "credentialDetails": {
                    "credentialType": 'SAS',
                    "credentials": json.dumps({"credentialData": [{"name": "token", "value": sas_token}]}),
                    "encryptedConnection": 'Encrypted',
                    "encryptionAlgorithm": "None",
                    "privacyLevel": "Organizational",
                    "useEndUserOAuth2Credentials": "False"
                }
            }
        else:
            raise Exception("======= UNKOWN DATASOURCE FOUND =========", datasource)

        url = f"{POWERBI_BASE_URL}/gateways/{datasource['gatewayId']}/datasources/{datasource['datasourceId']}"
        res = requests.patch(url, headers=_get_headers(client), data=json.dumps(credentials_update))
        if res.ok:
            print("--- credentials updated successfully ---")
        else:
            raise Exception("Failed to update credentials: ", res.content)


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


def add_usergroup_to_group(client, usergroup_id, usergroup_type, target_group_id):
    payload = {
        "identifier": usergroup_id, # "this has to be the id not the email of the group"
        "groupUserAccessRight": usergroup_type,  # or Viewer, Contributor, Admin
        "principalType": "Group"
    }

    headers = _get_headers(client)
    API_URL = f'https://api.powerbi.com/v1.0/myorg/groups/{target_group_id}/users'
    response = requests.post(API_URL, headers=headers, json=payload)
    if response.ok:
        print(f'--- {usergroup_id} added to group {target_group_id} as {usergroup_type}')
    else:
        raise Exception(f"--- failed to add user {usergroup_id} {response.content} ---")


def get_client(pbi_workspace_conn, scope_overrides=None):
    """
    returns a client object that can be used to interact with the PowerBI service.
    Also acquires a token for the Fabric REST API (same app registration, different
    resource scope) so the client can be used with the create_fabric_item_from_definition
    / update_fabric_item_definition helpers below.
    """
    config = _get_config(pbi_workspace_conn, scope_overrides)
    token = get_auth_token(config)
    fabric_config = _get_config(pbi_workspace_conn, ["https://api.fabric.microsoft.com/.default"])
    fabric_token = get_auth_token(fabric_config)
    client = {
        'auth_token': token,
        'fabric_auth_token': fabric_token,
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


INTEGRATION_HUB_REPOSITORY = "cienai/IntegrationHub"
GITHUB_API_VERSION = "2022-11-28"
GIT_LFS_MEDIA_TYPE = "application/vnd.git-lfs+json"


def _integration_hub_headers(github_token, *, accept="application/vnd.github+json"):
    if not github_token or not github_token.strip():
        raise ValueError("github_token is required to download IntegrationHub assets")
    return {
        "Authorization": f"Bearer {github_token.strip()}",
        "Accept": accept,
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }


def get_integration_hub_commit(ref, *, github_token):
    """Resolve an IntegrationHub branch, tag, or SHA to an immutable commit SHA."""
    url = f"https://api.github.com/repos/{INTEGRATION_HUB_REPOSITORY}/commits"
    response = requests.get(
        url,
        headers=_integration_hub_headers(github_token),
        params={"sha": ref, "per_page": 1},
        timeout=(10, 60),
    )
    response.raise_for_status()
    commits = response.json()
    if not isinstance(commits, list) or not commits or not commits[0].get("sha"):
        raise RuntimeError(f"No IntegrationHub commit found for ref {ref}")
    return commits[0]["sha"]


def _parse_git_lfs_pointer(content, filename):
    try:
        lines = content.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"IntegrationHub asset {filename} is not a Git LFS pointer") from exc

    values = {}
    for line in lines:
        key, separator, value = line.partition(" ")
        if separator:
            values[key] = value

    oid = values.get("oid", "")
    if values.get("version") != "https://git-lfs.github.com/spec/v1" or not oid.startswith("sha256:"):
        raise RuntimeError(f"IntegrationHub asset {filename} is not a valid Git LFS pointer")
    try:
        size = int(values["size"])
    except (KeyError, ValueError) as exc:
        raise RuntimeError(f"IntegrationHub asset {filename} has an invalid Git LFS size") from exc
    return oid.removeprefix("sha256:"), size


def _get_git_lfs_download_action(oid, size, *, github_token):
    url = f"https://github.com/{INTEGRATION_HUB_REPOSITORY}.git/info/lfs/objects/batch"
    headers = _integration_hub_headers(github_token, accept=GIT_LFS_MEDIA_TYPE)
    headers["Content-Type"] = GIT_LFS_MEDIA_TYPE
    response = requests.post(
        url,
        headers=headers,
        json={
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": oid, "size": size}],
        },
        timeout=(10, 60),
    )
    response.raise_for_status()
    objects = response.json().get("objects", [])
    if not objects:
        raise RuntimeError(f"Git LFS did not return object {oid}")
    lfs_object = objects[0]
    if lfs_object.get("error"):
        raise RuntimeError(f"Git LFS could not download object {oid}: {lfs_object['error']}")
    download_action = lfs_object.get("actions", {}).get("download")
    if not download_action or not download_action.get("href"):
        raise RuntimeError(f"Git LFS did not provide a download URL for object {oid}")
    return download_action


def download_file_from_integration_hub(tag, filename, local_file_name, *, github_token):
    """Download and verify a Git LFS-backed Power BI asset."""
    headers = _integration_hub_headers(
        github_token,
        accept="application/vnd.github.raw+json",
    )

    encoded_path = urllib.parse.quote(f"powerbi/{filename}", safe="/")
    pointer_url = f"https://api.github.com/repos/{INTEGRATION_HUB_REPOSITORY}/contents/{encoded_path}"
    partial_file_name = f"{local_file_name}.part"

    print(f"--- downloading IntegrationHub asset: {filename} at ref {tag}")
    try:
        pointer_response = requests.get(
            pointer_url,
            headers=headers,
            params={"ref": tag},
            timeout=(10, 60),
        )
        pointer_response.raise_for_status()
        oid, expected_size = _parse_git_lfs_pointer(pointer_response.content, filename)
        download_action = _get_git_lfs_download_action(
            oid,
            expected_size,
            github_token=github_token,
        )

        downloaded_size = 0
        digest = hashlib.sha256()
        with requests.get(
            download_action["href"],
            headers=download_action.get("header", {}),
            stream=True,
            timeout=(10, 300),
        ) as response:
            response.raise_for_status()
            with open(partial_file_name, "wb") as output_file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output_file.write(chunk)
                        downloaded_size += len(chunk)
                        digest.update(chunk)

        if downloaded_size != expected_size:
            raise RuntimeError(
                f"IntegrationHub asset {filename} size mismatch: "
                f"expected {expected_size}, downloaded {downloaded_size}"
            )
        if digest.hexdigest() != oid:
            raise RuntimeError(f"IntegrationHub asset {filename} checksum mismatch")

        os.replace(partial_file_name, local_file_name)
    except Exception:
        if os.path.exists(partial_file_name):
            os.remove(partial_file_name)
        raise


def checkout_integration_hub(tag, *, github_token):
    """
    Downloads the whole IntegrationHub repo at `tag` as a single tarball and extracts it
    to a local temp directory. One HTTP request regardless of repo size, versus the
    hundreds-to-thousands of individual Git Blobs API calls it'd take to fetch a PBIP
    project's files one at a time (which is both slow and burns through the GitHub REST
    API's hourly rate limit fast across a multi-tenant publish run). Also sidesteps the
    Git Trees API's truncation limit on very large repos.

    Returns the local path to the extracted repo root. Caller is responsible for cleaning
    it up (e.g. via `shutil.rmtree`) once done; reuse a single checkout across every
    dataset/report in a publish run rather than checking out per-item.
    """
    import shutil
    import tarfile
    import tempfile

    commit_sha = get_integration_hub_commit(tag, github_token=github_token)
    url = f"https://api.github.com/repos/{INTEGRATION_HUB_REPOSITORY}/tarball/{commit_sha}"
    headers = _integration_hub_headers(github_token)

    tmp_dir = tempfile.mkdtemp(prefix="integrationhub_")
    try:
        with requests.get(url, headers=headers, stream=True, timeout=(10, 300)) as response:
            response.raise_for_status()
            with tarfile.open(fileobj=response.raw, mode="r|gz") as tar:
                tar.extractall(tmp_dir, filter="data")
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    # GitHub tarballs have a single top-level "<owner>-<repo>-<short-sha>/" directory.
    entries = os.listdir(tmp_dir)
    if len(entries) != 1:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError(f"Unexpected IntegrationHub archive layout under {tmp_dir}: {entries}")
    return os.path.join(tmp_dir, entries[0])


def read_pbip_project_files(integration_hub_root, project_path):
    """
    Reads every file under `project_path` (e.g. "powerbi/src/<name>.SemanticModel" or
    "powerbi/src/<name>.Report") from a local IntegrationHub checkout produced by
    checkout_integration_hub.

    Returns {path relative to project_path: raw file bytes}.
    """
    project_dir = os.path.join(integration_hub_root, project_path)
    if not os.path.isdir(project_dir):
        raise RuntimeError(f"IntegrationHub path '{project_path}' not found under {integration_hub_root}")

    files = {}
    for dirpath, _dirnames, filenames in os.walk(project_dir):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(full_path, project_dir).replace(os.sep, "/")
            with open(full_path, "rb") as f:
                files[relative_path] = f.read()
    if not files:
        raise RuntimeError(f"No files found under IntegrationHub path '{project_path}'")
    return files


def build_semantic_model_definition_parts(files):
    """
    files: {relative path: raw bytes}, as returned by download_pbip_project_from_integration_hub
    for a *.SemanticModel project.

    Returns a Fabric definition.parts[] list containing just the files Fabric's semantic
    model item definition actually understands (definition.pbism, .platform, and everything
    under definition/) -- excluding Desktop-only convenience files like .pbi/, DAXQueries/,
    TMDLScripts/, diagramLayout.json.
    """
    parts = [
        {
            "path": path,
            "payload": base64.b64encode(data).decode("ascii"),
            "payloadType": "InlineBase64",
        }
        for path, data in files.items()
        if path in ("definition.pbism", ".platform") or path.startswith("definition/")
    ]
    if not parts:
        raise RuntimeError("No semantic model definition parts found (missing definition.pbism/definition/*)")
    return parts


def build_report_definition_parts(files, *, dataset_id):
    """
    files: {relative path: raw bytes}, as returned by download_pbip_project_from_integration_hub
    for a *.Report project.

    Returns a Fabric definition.parts[] list containing .platform, everything under
    definition/, StaticResources/ and CustomVisuals/ (reports that embed a custom visual
    reference their .pbiviz assets from there -- Fabric rejects the report at import time
    if they're missing), and a definition.pbir rewritten to bind the report to `dataset_id`
    -- so the created report is already bound to its dataset, no separate Rebind call
    needed. Desktop-editor-only files (DAXQueries/, semanticModelDiagramLayout.json, .pbi/)
    are intentionally excluded; they aren't part of Fabric's report definition schema.
    """
    included_prefixes = ("definition/", "StaticResources/", "CustomVisuals/")
    parts = [
        {
            "path": path,
            "payload": base64.b64encode(data).decode("ascii"),
            "payloadType": "InlineBase64",
        }
        for path, data in files.items()
        if path != "definition.pbir"
        and (path == ".platform" or path.startswith(included_prefixes))
    ]
    if not parts:
        raise RuntimeError("No report definition parts found (missing definition/* or StaticResources/*)")

    pbir = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": (
                    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/placeholder;"
                    'initial catalog="placeholder";access mode=readonly;'
                    f"integrated security=ClaimsToken;semanticmodelid={dataset_id}"
                )
            }
        },
    }
    pbir_bytes = json.dumps(pbir).encode("utf-8")
    parts.append({
        "path": "definition.pbir",
        "payload": base64.b64encode(pbir_bytes).decode("ascii"),
        "payloadType": "InlineBase64",
    })
    return parts


def _get_fabric_headers(client):
    return {
        "Authorization": f"Bearer {client['fabric_auth_token']}",
        "Content-Type": "application/json",
    }


def _poll_fabric_lro(headers, response, *, interval=20, max_polls=30):
    """Polls a Fabric long-running-operation response (202) until it succeeds or fails."""
    if response.status_code == 201:
        return response.json()
    if response.status_code != 202:
        raise Exception(f"Fabric API call failed: {response.status_code} {response.content}")

    op_url = response.headers["Location"]
    retry_after = int(response.headers.get("Retry-After", interval))
    for _ in range(max_polls):
        time.sleep(retry_after)
        poll_response = requests.get(op_url, headers=headers)
        poll_response.raise_for_status()
        status = poll_response.json().get("status")
        print(f"--- fabric operation status: {status} ---")
        if status == "Succeeded":
            result_response = requests.get(op_url.rstrip("/") + "/result", headers=headers)
            if result_response.status_code == 200 and result_response.text:
                return result_response.json()
            return {}
        if status == "Failed":
            raise Exception(f"Fabric operation failed: {poll_response.content}")
    raise TimeoutError(f"Timed out waiting for Fabric operation: {op_url}")


def create_fabric_item_from_definition(client, group_id, item_type, display_name, parts, description=None):
    """
    Creates a Fabric item (item_type: "semanticModels" or "reports") directly from a
    definition.parts[] payload (as built by build_semantic_model_definition_parts /
    build_report_definition_parts), skipping the classic pbix Import API entirely.
    """
    url = f"{FABRIC_BASE_URL}/workspaces/{group_id}/{item_type}"
    body = {"displayName": display_name, "definition": {"parts": parts}}
    if description:
        body["description"] = description
    headers = _get_fabric_headers(client)
    response = requests.post(url, headers=headers, data=json.dumps(body))
    return _poll_fabric_lro(headers, response)


def update_fabric_item_definition(client, group_id, item_type, item_id, parts):
    """
    Updates an existing Fabric item's definition in place (item_type: "semanticModels" or
    "reports").
    """
    url = f"{FABRIC_BASE_URL}/workspaces/{group_id}/{item_type}/{item_id}/updateDefinition"
    body = {"definition": {"parts": parts}}
    headers = _get_fabric_headers(client)
    response = requests.post(url, headers=headers, data=json.dumps(body))
    return _poll_fabric_lro(headers, response)


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
    print(f"--- adding group: {group_id} to capacity: {capacity_id} ---")
    print(f"--- add url: {api_url} ---")
    body = {'capacityId': capacity_id}
    response = requests.post(api_url, headers=_get_headers(client), data=json.dumps(body))
    if response.ok:
        print("--- add successful ---")
    else:
        raise Exception(f"--- add failed: {response.content} ---")


def disconnect_pbix(pbix_path):
    """
    Remove the Connections file from the given PBIX file
    """
    # files_to_remove = ['SecurityBindings', 'Connections']
    files_to_remove = ['Connections']
    # files_to_remove = ['SecurityBindings']
    # Create a temporary zip file
    temp_zip_path = pbix_path + '.temp'

    with zipfile.ZipFile(pbix_path, 'r') as zip_read:
        with zipfile.ZipFile(temp_zip_path, 'w') as zip_write:
            # Iterate over items in the original zip file
            for item in zip_read.infolist():
                if item.filename not in files_to_remove:
                    # Copy file to the new zip archive if it's not in the removal list
                    zip_write.writestr(item, zip_read.read(item.filename))

    # Replace the original zip file with the new one
    os.remove(pbix_path)
    os.rename(temp_zip_path, pbix_path)



def delete_group(client, group_id):
    """
    delete the group/workspace with the given group_id
    """
    delete_url = f"{POWERBI_BASE_URL}/groups/{group_id}"
    headers = _get_headers(client)
    del headers['Content-Type']
    response = requests.delete(delete_url, headers=headers)
    if response.ok:
        print("--- delete group successful ---")
    else:
        raise Exception(f"--- delete group failed: {response.content} ---")


def connect_pbix(pbix_path, group_id, dataset_id):
    """
    Connect the given PBIX file to the given group and dataset
    Warning: this uses undocumented code and may break in the future
    """
    connection_string = f"Data Source=pbiazure://api.powerbi.com;Initial Catalog={group_id};Identity Provider=\"https://login.microsoftonline.com/common, https://analysis.windows.net/powerbi/api, 7f67af8a-fedc-4b08-8b4e-37c4d127b6cf\";Integrated Security=ClaimsToken"
    content = {
        "Version": 3,
        "Connections": [
            {
                "Name": "EntityDataSource",
                "ConnectionString": connection_string,
                "ConnectionType": "pbiServiceLive",
                "PbiServiceModelId": 617430,
                "PbiModelVirtualServerName": "sobe_wowvirtualserver",
                "PbiModelDatabaseName": dataset_id
            }
        ]
    }
    content = json.dumps(content)
    temp_zip_path = pbix_path + '.temp'

    # Create a temporary ZIP file
    with zipfile.ZipFile(temp_zip_path, 'w') as zip_write:
        # Read from the original ZIP file
        with zipfile.ZipFile(pbix_path, 'r') as zip_read:
            # Copy existing files to the temporary ZIP file
            for item in zip_read.infolist():
                zip_write.writestr(item, zip_read.read(item.filename))
            # Write the new Connections file
            zip_write.writestr('Connections', content)

    # Replace the original ZIP file with the new one
    os.replace(temp_zip_path, pbix_path)
