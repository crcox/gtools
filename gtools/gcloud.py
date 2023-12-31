import pickle
import json
from collections import namedtuple
from collections.abc import Iterable
from pathlib import Path, PurePath
from google.cloud.storage import client, Blob, Bucket, transfer_manager
from datastructs import ModelInfo, ModelState


def ModelInfoFromBlob(blob: Blob) -> ModelInfo:
    with blob.open(mode="r") as f:
        x = json.load(f)

    x["name"] = blob.name
    x["bucket_name"] = Path(blob.bucket.path).name
    
    return ModelInfo._make(x[field] for field in ModelInfo._fields)


def path_relative_to_bucket_name(file: Path, bucket_name: str = None):
    if bucket_name is None:
        return file
    
    file_parts = Path(file).parts
    ind = file_parts.index(bucket_name)
    bucket_root = Path().joinpath(*file_parts[:ind+1])
    return str(Path(file).relative_to(bucket_root))

    
def ModelInfoFromFile(file: Path, bucket_name: str = None) -> ModelInfo:
    with open(file, mode="r") as f:
        x = json.load(f)

    x["name"] = path_relative_to_bucket_name(file, bucket_name)
    x["bucket_name"] = bucket_name
    
    return ModelInfo._make(x[field] for field in ModelInfo._fields)


def ModelStateFromBlob(blob: Blob) -> ModelState:
    with blob.open(mode="rb") as f:
        x = pickle.load(f)

    x["name"] = blob.name
    x["bucket_name"] = Path(blob.bucket.path).name
    
    return ModelState._make(x[field] for field in ModelState._fields)


def ModelStateFromFile(file: Path, bucket_name: str) -> ModelState:
    with open(mode="rb") as f:
        x = pickle.load(f)

    x["name"] = path_relative_to_bucket_name(file, bucket_name)
    x["bucket_name"] = bucket_name
    
    return ModelState._make(x[field] for field in ModelState._fields)


def BlobToFile(blob: Blob, destination_directory: Path = "") -> None:
    filename = Path(destination_directory) / Path(blob.bucket.path).name / Path(blob.name)
    filename.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(filename)


def ModelStateToFile(model_state: ModelState, bucket_name: str, destination_directory: Path = "") -> None:
    filename = Path(destination_directory) / Path(bucket_name) / Path(model_state.name)
    filename.parent.mkdir(parents=True, exist_ok=True)
    with open(filename, "wb") as f:
        pickle.dump({key: val for key,val in model_state._asdict().items() if key != "name"}, f)


def ModelInfoToFile(model_info: ModelInfo, bucket_name: str, destination_directory: Path = "") -> None:
    filename = Path(destination_directory) / Path(bucket_name) / Path(model_info.name)
    filename.parent.mkdir(parents=True, exist_ok=True)
    with open(filename, "w") as f:
        json.dump({key: val for key,val in model_info._asdict().items() if key != "name"},
                  f, ensure_ascii=False, indent=4)


def list_model_info_blobs(bucket: Bucket, run_name: str, as_list=True) -> list[Blob]:
    pattern = PurePath(run_name, "*", "config.json")
    blobs = bucket.list_blobs(match_glob=pattern)

    if as_list:
        return list(blobs)
    else:
        return blobs

def sort_modelinfo_by_lesion_onset(model_info: list[ModelInfo]) -> list[ModelInfo]:
    return sorted(model_info, key=lambda x: x.lesion_start_epoch)


def list_model_info(bucket: Bucket, run_name: str, sorted=True) -> list[ModelInfo]:
    pattern = PurePath(run_name, "*", "config.json")
    model_info = [ModelInfoFromBlob(blob)
                  for blob
                  in bucket.list_blobs(match_glob=pattern)]
    if sorted:
        return sort_modelinfo_by_lesion_onset(model_info)

    return model_info
        

def sort_epochs(epoch_blobs: list[Blob]) -> list[Blob]:
    def extract_epoch_count(blob: Blob) -> int:
        return int(PurePath(blob.name).name.replace(".","_").split("_")[1])

    labeled_epochs= [(blob, extract_epoch_count(blob)) for blob in epoch_blobs]

    return [x[0] for x in sorted(labeled_epochs, key=lambda x: x[1])]


def list_epoch_blobs(bucket: Bucket, run_name:str, wandb_id: str, sorted=True, as_list=True) -> Iterable[Blob]:
    pattern = PurePath(run_name, wandb_id, "states", "test", "production", "epoch_[0-9][0-9][0-9][0-9].pkl")
    blobs = bucket.list_blobs(match_glob=pattern)
    if sorted:
        return sort_epochs(list(blobs))
    
    if as_list:
        return list(blobs)
    else:
        return blobs


def list_epochs(bucket: Bucket, run_name: str, wandb_id: str, sorted=True) -> list[ModelState]:
    return [ModelStateFromBlob(blob)
            for blob in list_epoch_blobs(bucket, run_name, wandb_id, sorted)]


# The following two functions are taken from Google's documentation
def download_many_blobs_with_transfer_manager(
    bucket, blob_names, destination_directory="", workers=8
):
    """Download blobs in a list by name, concurrently in a process pool.

    The filename of each blob once downloaded is derived from the blob name and
    the `destination_directory `parameter. For complete control of the filename
    of each blob, use transfer_manager.download_many() instead.

    Directories will be created automatically as needed to accommodate blob
    names that include slashes.
    """

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The list of blob names to download. The names of each blobs will also
    # be the name of each destination file (use transfer_manager.download_many()
    # instead to control each destination file name). If there is a "/" in the
    # blob name, then corresponding directories will be created on download.
    # blob_names = ["myblob", "myblob2"]

    # The directory on your computer to which to download all of the files. This
    # string is prepended (with os.path.join()) to the name of each blob to form
    # the full path. Relative paths and absolute paths are both accepted. An
    # empty string means "the current working directory". Note that this
    # parameter allows accepts directory traversal ("../" etc.) and is not
    # intended for unsanitized end user input.
    # destination_directory = ""

    # The maximum number of processes to use for the operation. The performance
    # impact of this value depends on the use case, but smaller files usually
    # benefit from a higher number of processes. Each additional process occupies
    # some CPU and memory resources until finished. Threads can be used instead
    # of processes by passing `worker_type=transfer_manager.THREAD`.
    # workers=8

    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )

    for name, result in zip(blob_names, results):
        # The results list is either `None` or an exception for each blob in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to download {} due to exception: {}".format(name, result))
        else:
            print("Downloaded {} to {}.".format(name, destination_directory + name))


def download_bucket_with_transfer_manager(
    bucket, destination_directory="", workers=8, max_results=1000
):
    """Download all of the blobs in a bucket, concurrently in a process pool.

    The filename of each blob once downloaded is derived from the blob name and
    the `destination_directory `parameter. For complete control of the filename
    of each blob, use transfer_manager.download_many() instead.

    Directories will be created automatically as needed, for instance to
    accommodate blob names that include slashes.
    """

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The directory on your computer to which to download all of the files. This
    # string is prepended (with os.path.join()) to the name of each blob to form
    # the full path. Relative paths and absolute paths are both accepted. An
    # empty string means "the current working directory". Note that this
    # parameter allows accepts directory traversal ("../" etc.) and is not
    # intended for unsanitized end user input.
    # destination_directory = ""

    # The maximum number of processes to use for the operation. The performance
    # impact of this value depends on the use case, but smaller files usually
    # benefit from a higher number of processes. Each additional process occupies
    # some CPU and memory resources until finished. Threads can be used instead
    # of processes by passing `worker_type=transfer_manager.THREAD`.
    # workers=8

    # The maximum number of results to fetch from bucket.list_blobs(). This
    # sample code fetches all of the blobs up to max_results and queues them all
    # for download at once. Though they will still be executed in batches up to
    # the processes limit, queueing them all at once can be taxing on system
    # memory if buckets are very large. Adjust max_results as needed for your
    # system environment, or set it to None if you are sure the bucket is not
    # too large to hold in memory easily.
    # max_results=1000

    blob_names = [blob.name for blob in bucket.list_blobs(max_results=max_results)]

    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )

    for name, result in zip(blob_names, results):
        # The results list is either `None` or an exception for each blob in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to download {} due to exception: {}".format(name, result))
        else:
            print("Downloaded {} to {}.".format(name, destination_directory + name))


if __name__ == "__main__":
    import os

    # Primary setup
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/chriscox/.config/gcp_read_only.json"
    project_name = "time-varying-reader-1"
    bucket_name = "time-varying-reader-runs"
    gcs = client.Client(project=project_name)
    B = gcs.bucket(bucket_name=bucket_name)

    # Choose a run
    run_name = "s200_intact_freeze_phon"

    # Pull info to guide selection of model and epoch
    # N.B. `list_model_info()` returns ModelInfo tuples, which means data is
    # downloaded. `list_epoch_blobs` just returns references to blobs and a
    # little metadata, and the epoch state data must be retrieved later.
    model_info = list_model_info(B, run_name)
    wandb_id = model_info[-1].wandb_id
    epochs = list_epoch_blobs(B, run_name, wandb_id)

    # Download the state data
    #m = ModelStateFromBlob(epochs[-1])

    # Write downloaded data to file
    ModelInfoToFile(model_info[-1], bucket_name, "buckets")
    #ModelStateToFile(m, bucket_name, "buckets")

    # Write directly to file (picking a new model and epoch)
    model_info_blobs = list_model_info_blobs(B, run_name)
    config_from_blob = ModelInfoFromBlob(model_info_blobs[0])
    wandb_id = config_from_blob.wandb_id
    epochs = list_epoch_blobs(B, run_name, wandb_id)
    BlobToFile(model_info_blobs[0], "buckets")
    #BlobToFile(epochs[-1], "buckets")

    # Read from file
    config_from_file = ModelInfoFromFile(
        "buckets/time-varying-reader-runs/s200_intact_freeze_phon/1nexxgef/config.json",
        "time-varying-reader-runs")
    print(config_from_blob)
    print(config_from_file)
