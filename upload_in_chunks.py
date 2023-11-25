import io
from google.cloud import storage


def upload_blob_in_chunks_from_buffer(bucket_name, destination_blob_name, buffer, chunk_size):
    """Uploads a buffer to the bucket in chunks."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Konfigurieren des chunk_size. Needs to be a multiple of 256 KB
    blob.chunk_size = chunk_size * 1024 * 256  # Setzen Sie hier die Chunk-Größe in KB

    # Upload the buffer in chunks
    with io.BufferedReader(buffer) as reader:
        blob.upload_from_file(reader)

    print(f"Uploaded buffer to {bucket_name}/{destination_blob_name} in chunks.")
