# fdbfiles
Manipulate FoundationDB Object Store using the command line.

# Main features
- Implemented by adding an Object Storage layer on top of FoundationDB core data model
- Supports multiple versions of the same file: you upload the same file multiple times and both versions are available in the Object Store

# Current limitations
- Supports files up to 8192PiB in size
- Cancelling an upload will not result in a total rollback of the upload, but in a partial upload that will be consistent in the database and marked as a partial upload - files are uploaded in chunks using transactions
