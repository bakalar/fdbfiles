# fdbfiles
Manipulate FoundationDB object store using the command line.

# Main features
- Implementation of object storage and compression layer on top of FoundationDB core data model
- Supports multiple versions of the same object: you upload the object with the same name twice and both versions are available in the object store
- Transparent compression support (LZ4 algorithm): uploaded objects can be transparently compressed during upload
- Data model suitable for advanced operations like append, partial modification or partial download

# Current limitations
- Supports objects up to 8192PiB in size
- Cancelling an upload will not result in a total rollback of the upload, but in a partial upload that will be consistent in the database and marked as a partial upload - objects are uploaded in parts using transactions
