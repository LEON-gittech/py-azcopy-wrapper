from typing import List, Optional

from azcopy_wrapper.sas_token_validation import is_sas_token_session_expired


class LocationType:
    """
    This type is used to specify the location
    type of the object created for transfer
    for the AzCopy command
    """

    SRC = "source"
    DEST = "destination"


class AzRemoteSASLocation:
    """
    Class to create Azure Remote Location with SAS Token
    Returns the remote location url string with the information
    specified while creating the object
    """

    storage_account: str
    container: str
    path: str
    use_wildcard: bool
    sas_token: str
    location_type: Optional[str]

    def __init__(
        self,
        storage_account: str = "",
        container: str = "",
        path: str = "",
        use_wildcard: bool = False,
        sas_token: str = "",
        location_type: str = None,
    ) -> None:
        if len(sas_token) > 0:
            sas_token_expiry_flag = is_sas_token_session_expired(token=sas_token)

            if sas_token_expiry_flag == True:
                raise Exception("SAS token is expired")

        self.storage_account = storage_account
        self.container = container
        self.sas_token = sas_token
        self.use_wildcard = use_wildcard
        self.path = path
        self.location_type = location_type

    def get_resource_uri(self) -> str:
        return f"https://{self.storage_account}.blob.core.windows.net/{self.container}/"

    def __str__(self) -> str:
        """
        Creates the remote location url with sas token to be used for the final location
        """
        if len(self.sas_token) > 0:
            sas_token_expiry_flag = is_sas_token_session_expired(token=self.sas_token)

            if sas_token_expiry_flag == True:
                raise Exception("SAS token is expired")

        resource_uri = self.get_resource_uri()

        wildcard = ""
        if self.use_wildcard == True:
            wildcard = "*"

        return resource_uri + self.path + wildcard + "?" + self.sas_token


class AzLocalLocation:
    """
    Class to create Local Path for data transfer using Azcopy
    """

    path: str
    use_wildcard: bool
    location_type: Optional[str]

    def __init__(
        self,
        path: str = "",
        use_wildcard: bool = False,
        location_type: str = None,
    ) -> None:
        self.path = path
        self.use_wildcard = use_wildcard
        self.location_type = location_type

    def __str__(self) -> str:
        wildcard = ""
        if self.use_wildcard == True:
            wildcard = "*"

        return self.path + wildcard


class AzCopyOptions:
    """
    Class to give specific options for data transfer using Azcopy
    """

    overwrite_existing: bool
    recursive: bool
    put_md5: bool
    exclude_path: str

    def __init__(
        self,
        overwrite_existing: bool = False,
        recursive: bool = False,
        put_md5: bool = False,
        exclude_path: str = "",
    ) -> None:
        self.overwrite_existing = overwrite_existing
        self.recursive = recursive
        self.put_md5 = put_md5
        self.exclude_path = exclude_path

    def get_options_list(self) -> List[str]:
        transfer_options = []

        # Look into subdirectories recursively when transferring
        if self.recursive:
            transfer_options.append("--recursive")

        # Overwrite the conflicting files and blobs at the destination if this flag is set to true. (default true)
        if not self.overwrite_existing:
            transfer_options.append("--overwrite")
            transfer_options.append("false")

        # Create an MD5 hash of each file, and save the hash as the Content-MD5 property
        # of the destination blob or file.
        # Only available for uploading
        if self.put_md5:
            transfer_options.append("--put-md5")

        # Exclude these paths when copying.
        if len(self.exclude_path) > 0:
            transfer_options.append("--exclude-path")
            transfer_options.append(self.exclude_path)

        return transfer_options


class AzSyncOptions:
    """
    Class to give specific options for data transfer using Azcopy
    """

    recursive: bool
    put_md5: bool
    exclude_path: str

    def __init__(
        self,
        recursive: bool = False,
        put_md5: bool = False,
        exclude_path: str = "",
    ) -> None:
        self.recursive = recursive
        self.put_md5 = put_md5
        self.exclude_path = exclude_path

    def get_options_list(self) -> List[str]:
        transfer_options = []

        # Look into subdirectories recursively when transferring
        if self.recursive:
            transfer_options.append("--recursive")

        # Create an MD5 hash of each file, and save the hash as the Content-MD5 property
        # of the destination blob or file.
        if self.put_md5:
            transfer_options.append("--put-md5")

        if len(self.exclude_path) > 0:
            transfer_options.append("--exclude-path")
            transfer_options.append(self.exclude_path)

        return transfer_options


class AzCopyJobInfo:
    """
    Created the job info of the Azcopy job executed by the user
    """

    percent_complete: float
    error_msg: str
    number_of_file_transfers: int
    number_of_folder_property_transfers: int
    total_number_of_transfers: int
    number_of_transfers_completed: int
    number_of_transfers_failed: int
    number_of_transfers_skipped: int
    total_bytes_transferred: int
    final_job_status_msg: str
    completed: bool

    def __init__(
        self,
        percent_complete: float = float(0),
        error_msg: str = "",
        final_job_status_msg: str = "",
        number_of_file_transfers: int = 0,
        number_of_folder_property_transfers: int = 0,
        total_number_of_transfers: int = 0,
        number_of_transfers_completed: int = 0,
        number_of_transfers_failed: int = 0,
        number_of_transfers_skipped: int = 0,
        total_bytes_transferred: int = 0,
        completed: bool = False,
    ) -> None:
        # NOTE: Sometimes, azcopy doesn't return value as 100%
        # even if the entire data is transferred.
        # This might be because if the transfer is completed in between
        # the value sent by azcopy, then azcopy fails to send the final
        # percent value and directly sends the job summary
        self.percent_complete = percent_complete
        self.error_msg = error_msg
        self.final_job_status_msg = final_job_status_msg
        self.number_of_file_transfers = number_of_file_transfers
        self.number_of_folder_property_transfers = number_of_folder_property_transfers
        self.total_number_of_transfers = total_number_of_transfers
        self.number_of_transfers_completed = number_of_transfers_completed
        self.number_of_transfers_failed = number_of_transfers_failed
        self.number_of_transfers_skipped = number_of_transfers_skipped
        self.total_bytes_transferred = total_bytes_transferred
        self.completed = completed


class AzSyncJobInfo:
    """
    Created the job info of the Azcopy job executed by the user
    """

    percent_complete: float
    error_msg: str
    files_scanned_at_source: int
    files_scanned_at_destination: int
    # elapsed_time_minutes: float
    number_of_copy_transfers_for_files: int
    number_of_copy_transfers_for_folder_properties: int
    number_of_folder_property_transfers: int
    total_number_of_copy_transfers: int
    number_of_copy_transfers_completed: int
    number_of_copy_transfers_failed: int
    number_of_deletions_at_destination: int
    total_number_of_bytes_transferred: int
    total_number_of_bytes_enumerated: int
    final_job_status_msg: str
    completed: bool

    def __init__(
        self,
        percent_complete: float = float(0),
        error_msg: str = "",
        files_scanned_at_source: int = 0,
        files_scanned_at_destination: int = 0,
        # elapsed_time_minutes: float = float(0),
        number_of_copy_transfers_for_files: int = 0,
        number_of_copy_transfers_for_folder_properties: int = 0,
        number_of_folder_property_transfers: int = 0,
        total_number_of_copy_transfers: int = 0,
        number_of_copy_transfers_completed: int = 0,
        number_of_copy_transfers_failed: int = 0,
        number_of_deletions_at_destination: int = 0,
        total_number_of_bytes_transferred: int = 0,
        total_number_of_bytes_enumerated: int = 0,
        final_job_status_msg: str = "",
        completed: bool = False,
    ) -> None:
        # NOTE: Sometimes, azcopy doesn't return value as 100%
        # even if the entire data is transferred.
        # This might be because if the transfer is completed in between
        # the value sent by azcopy, then azcopy fails to send the final
        # percent value and directly sends the job summary
        self.percent_complete = percent_complete
        self.error_msg = error_msg
        self.final_job_status_msg = final_job_status_msg
        self.files_scanned_at_source = files_scanned_at_source
        self.files_scanned_at_destination = files_scanned_at_destination
        # self.elapsed_time_minutes = elapsed_time_minutes
        self.number_of_copy_transfers_for_files = number_of_copy_transfers_for_files
        self.number_of_copy_transfers_for_folder_properties = (
            number_of_copy_transfers_for_folder_properties
        )
        self.number_of_folder_property_transfers = number_of_folder_property_transfers
        self.total_number_of_copy_transfers = total_number_of_copy_transfers
        self.number_of_copy_transfers_completed = number_of_copy_transfers_completed
        self.number_of_copy_transfers_failed = number_of_copy_transfers_failed
        self.number_of_deletions_at_destination = number_of_deletions_at_destination
        self.total_number_of_bytes_transferred = total_number_of_bytes_transferred
        self.total_number_of_bytes_enumerated = total_number_of_bytes_enumerated
        self.completed = completed


class AzListOptions:
    """
    Class to give specific options for listing files using Azcopy list command
    """

    properties: Optional[str]
    output_type: str
    output_level: Optional[str]
    machine_readable: bool
    mega_units: bool
    running_tally: bool
    trailing_dot: Optional[str]

    def __init__(
        self,
        properties: Optional[str] = None,
        output_type: str = "text",
        output_level: Optional[str] = None,
        machine_readable: bool = False,
        mega_units: bool = False,
        running_tally: bool = False,
        trailing_dot: Optional[str] = None,
    ) -> None:
        self.properties = properties
        self.output_type = output_type
        self.output_level = output_level
        self.machine_readable = machine_readable
        self.mega_units = mega_units
        self.running_tally = running_tally
        self.trailing_dot = trailing_dot

    def get_options_list(self) -> List[str]:
        list_options = []

        # Specify properties to display (semicolon separated in double quotes)
        if self.properties:
            list_options.append("--properties")
            list_options.append(f'"{self.properties}"')

        # Output format type
        if self.output_type != "text":
            list_options.append("--output-type")
            list_options.append(self.output_type)

        # Output level
        if self.output_level:
            list_options.append("--output-level")
            list_options.append(self.output_level)

        # Machine readable format
        if self.machine_readable:
            list_options.append("--machine-readable")

        # Mega units (1000 instead of 1024)
        if self.mega_units:
            list_options.append("--mega-units")

        # Count files and total size
        if self.running_tally:
            list_options.append("--running-tally")

        # Trailing dot handling
        if self.trailing_dot:
            list_options.append("--trailing-dot")
            list_options.append(self.trailing_dot)

        return list_options


class AzListJobInfo:
    """
    Store the result information from azcopy list command
    """

    error_msg: str
    final_job_status_msg: str
    completed: bool
    output_text: str
    items: List[dict]

    def __init__(
        self,
        error_msg: str = "",
        final_job_status_msg: str = "",
        completed: bool = False,
        output_text: str = "",
        items: Optional[List[dict]] = None,
    ) -> None:
        self.error_msg = error_msg
        self.final_job_status_msg = final_job_status_msg
        self.completed = completed
        self.output_text = output_text
        self.items = items or []


class AzRemoveOptions:
    """
    Class to give specific options for data removal using Azcopy remove command
    """

    recursive: bool
    include_pattern: Optional[str]
    exclude_pattern: Optional[str]
    dry_run: bool
    delete_snapshots: Optional[str]
    list_of_files: Optional[str]
    list_of_versions: Optional[str]
    force_if_read_only: bool
    permanent_delete: bool
    include_after: Optional[str]
    include_before: Optional[str]

    def __init__(
        self,
        recursive: bool = False,
        include_pattern: Optional[str] = None,
        exclude_pattern: Optional[str] = None,
        dry_run: bool = False,
        delete_snapshots: Optional[str] = None,
        list_of_files: Optional[str] = None,
        list_of_versions: Optional[str] = None,
        force_if_read_only: bool = False,
        permanent_delete: bool = False,
        include_after: Optional[str] = None,
        include_before: Optional[str] = None,
    ) -> None:
        self.recursive = recursive
        self.include_pattern = include_pattern
        self.exclude_pattern = exclude_pattern
        self.dry_run = dry_run
        self.delete_snapshots = delete_snapshots
        self.list_of_files = list_of_files
        self.list_of_versions = list_of_versions
        self.force_if_read_only = force_if_read_only
        self.permanent_delete = permanent_delete
        self.include_after = include_after
        self.include_before = include_before

    def get_options_list(self) -> List[str]:
        remove_options = []

        # Look into subdirectories recursively when removing
        if self.recursive:
            remove_options.append("--recursive")

        # Include only files whose names match the pattern
        if self.include_pattern:
            remove_options.append("--include-pattern")
            remove_options.append(self.include_pattern)

        # Exclude files whose names match the pattern
        if self.exclude_pattern:
            remove_options.append("--exclude-pattern")
            remove_options.append(self.exclude_pattern)

        # Show what would be removed without actually removing
        if self.dry_run:
            remove_options.append("--dry-run")

        # Delete snapshots (include, only, none)
        if self.delete_snapshots:
            remove_options.append("--delete-snapshots")
            remove_options.append(self.delete_snapshots)

        # List of files to be removed from the source
        if self.list_of_files:
            remove_options.append("--list-of-files")
            remove_options.append(self.list_of_files)

        # List of versions to be removed from the source
        if self.list_of_versions:
            remove_options.append("--list-of-versions")
            remove_options.append(self.list_of_versions)

        # Force remove even if the file is read-only
        if self.force_if_read_only:
            remove_options.append("--force-if-read-only")

        # Delete permanently without moving to recycle bin
        if self.permanent_delete:
            remove_options.append("--permanent-delete")

        # Include only files whose last modified time is on or after the given value
        if self.include_after:
            remove_options.append("--include-after")
            remove_options.append(self.include_after)

        # Include only files whose last modified time is on or before the given value
        if self.include_before:
            remove_options.append("--include-before")
            remove_options.append(self.include_before)

        return remove_options


class AzRemoveJobInfo:
    """
    Store the job info of the Azcopy remove job executed by the user
    """

    percent_complete: float
    error_msg: str
    final_job_status_msg: str
    completed: bool
    number_of_files_removed: int
    number_of_folders_removed: int
    total_number_of_removals: int
    number_of_removals_completed: int
    number_of_removals_failed: int
    number_of_removals_skipped: int
    total_bytes_removed: int

    def __init__(
        self,
        percent_complete: float = float(0),
        error_msg: str = "",
        final_job_status_msg: str = "",
        completed: bool = False,
        number_of_files_removed: int = 0,
        number_of_folders_removed: int = 0,
        total_number_of_removals: int = 0,
        number_of_removals_completed: int = 0,
        number_of_removals_failed: int = 0,
        number_of_removals_skipped: int = 0,
        total_bytes_removed: int = 0,
    ) -> None:
        self.percent_complete = percent_complete
        self.error_msg = error_msg
        self.final_job_status_msg = final_job_status_msg
        self.completed = completed
        self.number_of_files_removed = number_of_files_removed
        self.number_of_folders_removed = number_of_folders_removed
        self.total_number_of_removals = total_number_of_removals
        self.number_of_removals_completed = number_of_removals_completed
        self.number_of_removals_failed = number_of_removals_failed
        self.number_of_removals_skipped = number_of_removals_skipped
        self.total_bytes_removed = total_bytes_removed
