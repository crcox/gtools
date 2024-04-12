from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """

    SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
    service_account_credentials = "/home/chriscox/.config/gcp_read_only.json"
    sa_creds = Credentials.from_service_account_file(service_account_credentials)
    scoped_creds = sa_creds.with_scopes(SCOPES)

    try:
        service = build("drive", "v3", credentials=scoped_creds)

        # Call the Drive v3 API
        results = (
            service.files()
            .list(pageSize=10, fields="nextPageToken, files(id, name)")
            .execute()
        )
        items = results.get("files", [])

        if not items:
            print("No files found.")
            return

        print("Files:")
        for item in items:
            print(f"{item['name']} ({item['id']})")

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")


if __name__ == "__main__":
  main()