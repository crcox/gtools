
Space for working in remote Google containers (GCS, BQ, and Drive)

# Dependencies
This repository defines wrappers around Google's `google-cloud-storage` python API, so you must install that into your virtual environment:

```bash
pip install google-cloud-storage
```

# Authentication options
In order to transact with the various Google services, you will need to authenticate. There are several ways of doing this.

## gcloud CLI
One way is using the [**gcloud CLI**](https://cloud.google.com/docs/authentication/gcloud#local), which works well when working on local workstation. If you are working on a remote maching over SSH, authentication using **gcloud CLI** may be thwarted by the need to access a web browser (my attempts to use X-window forwarding failed).

## gcp json file
In that case, you may need to use the `gcp_read_only.json`. To make this your default authentication token, set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

In bash (add to `.bashrc` to set this value for every new instance of the bash shell):

```bash
export GOOGLE_APPLICATION_CREDENTIALS=path/to/gcp_read_only.json
```

In python:

```python
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/gcp_read_only.json"
```

# Instantiate a client and interact with buckets
These procedures are not wrapped, because they are so fundamental. Whenever you want to interact with Google Cloud Storage, you will need to establish a [**client**](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client). Once the client is established, you can point to a [**bucket**](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.bucket).

For example:

```python
gcs = client.Client(project="time-varying-reader-1")
B = gcs.bucket(bucket_name="time-varying-reader-runs")
```

The functions in this repository expect that you are passing a pointer to a bucket as an argument.