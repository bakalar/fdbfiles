# fdbfiles
Manipulate FoundationDB Object Store using the command line.

# Main features
- Implemented object storage and compression layer on top of FoundationDB core data model
- Supports multiple versions of the same file: you upload the same file multiple times and both versions are available in the Object Store
- Transparent compression support (LZ4 algorithm): uploaded files can be transparently compressed during upload
- Implements data model suitable for advanced operations like partial modifications or partial download

# Current limitations
- Supports files/objects up to 8192PiB in size
- Cancelling an upload will not result in a total rollback of the upload, but in a partial upload that will be consistent in the database and marked as a partial upload - files are uploaded in chunks using transactions
