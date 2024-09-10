import base64
import requests

def upload_files_to_github(files, repo, token, message="Update files"):
    for local_path, repo_path in files.items():
        url = f"https://api.github.com/repos/{repo}/contents/{repo_path}"

        # Get the SHA of the existing file, if it exists
        response = requests.get(url, headers={"Authorization": f"token {token}"})
        if response.status_code == 200:
            sha = response.json()['sha']
        else:
            sha = None

        with open(local_path, "rb") as f:
            content = base64.b64encode(f.read()).decode()

        # Check if the content has changed
        if sha:
            existing_content = requests.get(url, headers={"Authorization": f"token {token}"}).json()['content']
            if content == existing_content:
                print(f"No changes detected in {local_path}. Skipping upload.")
                continue

        data = {
            "message": message,
            "content": content,
            "branch": "main",
        }
        
        if sha:
            data["sha"] = sha

        headers = {
            "Authorization": f"token {token}",
            "Content-Type": "application/json"
        }

        response = requests.put(url, json=data, headers=headers)
        response.raise_for_status()
        
# # Usage example
# upload_files_to_github(
#     files={
#         "/path/to/your/data_expanded_for_model.parquet": "data/data_expanded_for_model.parquet",
#         "/path/to/your/another_file.parquet": "data/another_file.parquet"
#     },
#     repo="Romanos-Rizk/AUB-capstone",
#     token="your_personal_access_token"
# )
