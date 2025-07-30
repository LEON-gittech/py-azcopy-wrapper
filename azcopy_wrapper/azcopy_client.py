import re
import os
import warnings

from typing import Optional, Union
from azcopy_wrapper.azcopy_summary import (
    get_transfer_copy_summary_info,
    get_sync_summary_info,
    get_remove_summary_info,
)
from azcopy_wrapper.azcopy_utilities import (
    AzCopyJobInfo,
    AzCopyOptions,
    AzLocalLocation,
    AzRemoteSASLocation,
    AzSyncJobInfo,
    AzSyncOptions,
    AzListOptions,
    AzListJobInfo,
    AzRemoveOptions,
    AzRemoveJobInfo,
    LocationType,
)
from azcopy_wrapper.sas_token_validation import is_sas_token_session_expired
from azcopy_wrapper.utils.execute_command import execute_command


class AzClient:
    """
    Azcopy client to execute commands for the user
    If azcopy is already installed in the system, then the client will directly use that exe

    For ex. If the azcopy binary file exists in /usr/local/bin/, then client will by default use that file.

    But if the usr wants to use another specific file which is stored in some other location, then they will have to
    specify it while creating the AzClient object
    """

    exe_to_use: str
    artefact_dir: Optional[str]

    def __init__(
        self, exe_to_use: str = "azcopy", artefact_dir: Optional[str] = None
    ) -> None:
        self.exe_to_use = exe_to_use
        self.artefact_dir = artefact_dir

    def _copy(
        self,
        src: Union[AzRemoteSASLocation, AzLocalLocation],
        dest: Union[AzRemoteSASLocation, AzLocalLocation],
        transfer_options: AzCopyOptions,
    ) -> AzCopyJobInfo:
        """
        Copies that data from source to destionation
        with the transfer options specified
        """
        import re

        # Generating the command to be used for subprocess
        cmd = [
            self.exe_to_use,
            "cp",
            str(src),
            str(dest),
        ] + transfer_options.get_options_list()

        # Creating AzCopyJobInfo object to store the job info
        job_info = AzCopyJobInfo()

        try:
            summary = ""
            # A boolean flag to be set as True when
            # azcopy starts sending summary information
            unlock_summary = False

            for output_line in execute_command(cmd):
                print(output_line, end="")

                # Extracting the percent complete information from the
                # current output line and updating it in the job_info
                if "%" in output_line:

                    percent_expression = r"(?P<percent_complete>\d+\.\d+) %,"
                    transfer_match = re.match(percent_expression, output_line)

                    if transfer_match is not None:
                        transfer_info = transfer_match.groupdict()

                        job_info.percent_complete = float(
                            transfer_info["percent_complete"]
                        )

                # If azcopy has started sending summary then
                # appending it to summary text
                if unlock_summary:
                    summary += output_line

                # Job summary starts with line ->
                # Job {job_id} summary
                if output_line.startswith("Job") and "summary" in output_line:
                    unlock_summary = True

                if "AuthenticationFailed" in output_line:
                    job_info.error_msg = output_line

                if "Final Job Status:" in output_line:
                    job_info.final_job_status_msg = output_line.split(":")[-1].strip()

        except Exception as e:
            # Checking if the error is because of the sas token

            if type(dest) == AzRemoteSASLocation:
                token = str(dest.sas_token)
            elif type(src) == AzRemoteSASLocation:
                token = str(src.sas_token)
            else:
                token = ""

            token_expiry_flag = is_sas_token_session_expired(token)

            if token_expiry_flag == True:
                job_info.error_msg = "SAS token is expired"
            else:
                job_info.error_msg = str(e)

            job_info.completed = False

        # Get the final job summary info
        job_info = get_transfer_copy_summary_info(job_info, summary)

        if (
            job_info.final_job_status_msg == "Completed"
            or job_info.final_job_status_msg == "CompletedWithSkipped"
        ):
            job_info.completed = True
        elif job_info.number_of_transfers_failed > 0:
            job_info.error_msg += "; Tranfers failed = {}".format(
                job_info.number_of_transfers_failed
            )
            job_info.completed = False
            raise Exception(job_info.error_msg)
        else:
            job_info.error_msg += "; Error while transferring data"
            job_info.completed = False
            raise Exception(job_info.error_msg)

        return job_info

    def _sync(
        self,
        src: Union[AzRemoteSASLocation, AzLocalLocation],
        dest: Union[AzRemoteSASLocation, AzLocalLocation],
        transfer_options: AzSyncOptions,
    ) -> AzSyncJobInfo:
        """
        Syncs that data from source to destionation
        with the transfer options specified
        """
        # Generating the command to be used for subprocess
        cmd = [
            self.exe_to_use,
            "sync",
            str(src),
            str(dest),
        ] + transfer_options.get_options_list()

        # Creating AzSyncJobInfo object to store the job info
        job_info = AzSyncJobInfo()

        try:
            summary = ""
            # A boolean flag to be set as True when
            # azcopy starts sending summary information
            unlock_summary = False

            for output_line in execute_command(cmd):
                print(output_line, end="")

                # Extracting the percent complete information from the
                # current output line and updating it in the job_info
                if "%" in output_line:

                    percent_expression = r"(?P<percent_complete>\d+\.\d+) %,"
                    transfer_match = re.match(percent_expression, output_line)

                    if transfer_match is not None:
                        transfer_info = transfer_match.groupdict()

                        job_info.percent_complete = float(
                            transfer_info["percent_complete"]
                        )

                # If azcopy has started sending summary then
                # appending it to summary text
                if unlock_summary:
                    summary += output_line.replace("(", "").replace(")", "")

                # Job summary starts with line ->
                # Job {job_id} summary
                output_line_cleaned = output_line.strip().lower()

                if (
                    output_line_cleaned.startswith("job")
                    and "summary" in output_line_cleaned
                ):
                    unlock_summary = True

                if "AuthenticationFailed" in output_line:
                    job_info.error_msg = output_line

                if "Final Job Status:" in output_line:
                    job_info.final_job_status_msg = output_line.split(":")[-1].strip()

        except Exception as e:
            job_info.completed = False

        # Get the final job summary info
        job_info = get_sync_summary_info(job_info, summary)

        if (
            job_info.final_job_status_msg == "Completed"
            or job_info.final_job_status_msg == "CompletedWithSkipped"
        ):
            job_info.completed = True
        elif job_info.number_of_copy_transfers_failed > 0:
            job_info.error_msg += "; Tranfers failed = {}".format(
                job_info.number_of_copy_transfers_failed
            )
            job_info.completed = False
            raise Exception(job_info.error_msg)
        else:
            job_info.error_msg += "; Error while transferring data"
            job_info.completed = False
            raise Exception(job_info.error_msg)

        return job_info

    def _list(
        self,
        location: AzRemoteSASLocation,
        list_options: AzListOptions,
    ) -> AzListJobInfo:
        """
        Lists files and directories from a remote location
        with the list options specified
        """
        import json

        # Generating the command to be used for subprocess
        cmd = [
            self.exe_to_use,
            "list",
            str(location),
        ] + list_options.get_options_list()

        # Creating AzListJobInfo object to store the job info
        job_info = AzListJobInfo()
        output_lines = []

        try:
            for output_line in execute_command(cmd):
                print(output_line, end="")
                output_lines.append(output_line.rstrip())

                # Check for authentication errors
                if "AuthenticationFailed" in output_line:
                    job_info.error_msg = output_line
                    job_info.completed = False
                    raise Exception(job_info.error_msg)

            # Join all output lines
            job_info.output_text = "\n".join(output_lines)

            # If output type is JSON, try to parse it
            if list_options.output_type == "json":
                try:
                    # AzCopy returns NDJSON format (one JSON object per line)
                    items = []
                    for line in output_lines:
                        line = line.strip()
                        if line and line.startswith('{') and line.endswith('}'):
                            try:
                                json_obj = json.loads(line)
                                # Extract the actual file info from MessageContent if it's a ListObject
                                if json_obj.get("MessageType") == "ListObject":
                                    message_content = json_obj.get("MessageContent", "")
                                    if message_content:
                                        file_info = json.loads(message_content)
                                        items.append(file_info)
                                elif json_obj.get("MessageType") != "EndOfJob":
                                    # Include other non-EndOfJob message types
                                    items.append(json_obj)
                            except json.JSONDecodeError:
                                continue
                    job_info.items = items
                except Exception:
                    # If JSON parsing fails, keep the raw text
                    pass

            job_info.completed = True
            job_info.final_job_status_msg = "Completed"

        except Exception as e:
            if job_info.error_msg == "":
                job_info.error_msg = str(e)
            job_info.completed = False
            raise Exception(job_info.error_msg)

        return job_info

    def _remove(
        self,
        location: AzRemoteSASLocation,
        remove_options: AzRemoveOptions,
    ) -> AzRemoveJobInfo:
        """
        Removes files and directories from the remote location
        with the remove options specified
        """
        import re

        # Generating the command to be used for subprocess
        cmd = [
            self.exe_to_use,
            "remove",
            str(location),
        ] + remove_options.get_options_list()

        # Creating AzRemoveJobInfo object to store the job info
        job_info = AzRemoveJobInfo()

        try:
            summary = ""
            # A boolean flag to be set as True when
            # azcopy starts sending summary information
            unlock_summary = False

            for output_line in execute_command(cmd):
                print(output_line, end="")

                # Extracting the percent complete information from the
                # current output line and updating it in the job_info
                if "%" in output_line:

                    percent_expression = r"(?P<percent_complete>\d+\.\d+) %,"
                    transfer_match = re.match(percent_expression, output_line)

                    if transfer_match is not None:
                        transfer_info = transfer_match.groupdict()

                        job_info.percent_complete = float(
                            transfer_info["percent_complete"]
                        )

                # If azcopy has started sending summary then
                # appending it to summary text
                if unlock_summary:
                    summary += output_line

                # Job summary starts with line ->
                # Job {job_id} summary
                if output_line.startswith("Job") and "summary" in output_line:
                    unlock_summary = True

                if "AuthenticationFailed" in output_line:
                    job_info.error_msg = output_line

                if "Final Job Status:" in output_line:
                    job_info.final_job_status_msg = output_line.split(":")[-1].strip()

        except Exception as e:
            # Checking if the error is because of the sas token
            token = str(location.sas_token)
            token_expiry_flag = is_sas_token_session_expired(token)

            if token_expiry_flag == True:
                job_info.error_msg = "SAS token is expired"
            else:
                job_info.error_msg = str(e)

            job_info.completed = False

        # Get the final job summary info
        job_info = get_remove_summary_info(job_info, summary)

        if (
            job_info.final_job_status_msg == "Completed"
            or job_info.final_job_status_msg == "CompletedWithSkipped"
        ):
            job_info.completed = True
        elif job_info.number_of_removals_failed > 0:
            job_info.error_msg += "; Removals failed = {}".format(
                job_info.number_of_removals_failed
            )
            job_info.completed = False
            raise Exception(job_info.error_msg)
        else:
            job_info.error_msg += "; Error while removing data"
            job_info.completed = False
            raise Exception(job_info.error_msg)

        return job_info

    # def download_file_to_local_path(
    #     self,
    #     src: AzRemoteSASLocation,
    #     dest: AzLocalLocation,
    #     transfer_options: AzCopyOptions,
    # ) -> AzCopyJobInfo:
    #     return self._copy(src=src, dest=dest, transfer_options=transfer_options)

    # def upload_file_to_remote_path(
    #     self,
    #     src: AzRemoteSASLocation,
    #     dest: AzLocalLocation,
    #     transfer_options: AzCopyOptions,
    # ) -> AzCopyJobInfo:
    #     return self._copy(src=src, dest=dest, transfer_options=transfer_options)

    # def upload_directory_to_remote_path(
    #     self,
    #     src: AzLocalLocation,
    #     dest: AzRemoteSASLocation,
    #     transfer_options: AzCopyOptions,
    # ) -> AzCopyJobInfo:
    #     src.location_type = LocationType.SRC
    #     dest.location_type = LocationType.DEST

    #     if transfer_options.recursive != True and src.use_wildcard != True:
    #         raise Exception(
    #             "Cannot use directory as source without --recursive or a trailing wildcard (/*)"
    #         )

    #     return self._copy(src=src, dest=dest, transfer_options=transfer_options)

    ####################################################################
    # Copy Data
    ####################################################################

    def download_data_to_local_location(
        self,
        src: AzRemoteSASLocation,
        dest: AzLocalLocation,
        transfer_options: AzCopyOptions,
    ) -> AzCopyJobInfo:
        return self._copy(src=src, dest=dest, transfer_options=transfer_options)

    def upload_data_to_remote_location(
        self,
        src: AzRemoteSASLocation,
        dest: AzLocalLocation,
        transfer_options: AzCopyOptions,
    ) -> AzCopyJobInfo:
        return self._copy(src=src, dest=dest, transfer_options=transfer_options)

    def copy_remote_data_from_container_to_container(
        self,
        src: AzRemoteSASLocation,
        dest: AzRemoteSASLocation,
        transfer_options: AzCopyOptions,
    ) -> AzCopyJobInfo:
        return self._copy(src=src, dest=dest, transfer_options=transfer_options)

    ####################################################################
    # Sync Data
    ####################################################################

    def sync_to_local_location(
        self,
        src: AzRemoteSASLocation,
        dest: AzLocalLocation,
        transfer_options: AzSyncOptions,
    ) -> AzSyncJobInfo:
        if not os.path.exists(dest.path):
            raise Exception(
                f"{dest.path} does not exist. For sync operation, the given path needs to exist"
            )

        return self._sync(src=src, dest=dest, transfer_options=transfer_options)

    def sync_to_remote_location(
        self,
        src: AzLocalLocation,
        dest: AzRemoteSASLocation,
        transfer_options: AzSyncOptions,
    ) -> AzSyncJobInfo:
        if not os.path.exists(src.path):
            raise Exception(
                f"{src.path} does not exist. For sync operation, the given path needs to exist"
            )

        return self._sync(src=src, dest=dest, transfer_options=transfer_options)

    ####################################################################
    # List Data

    def list_remote_location(
        self,
        location: AzRemoteSASLocation,
        list_options: AzListOptions,
    ) -> AzListJobInfo:
        """
        Lists files and directories from a remote Azure Storage location.
        
        Args:
            location: The remote Azure storage location to list
            list_options: Options for the list operation (output format, properties, etc.)
            
        Returns:
            AzListJobInfo containing the list results
            
        Example:
            remote_location = AzRemoteSASLocation(
                storage_account="mystorageaccount",
                container="mycontainer", 
                path="myfolder/",
                sas_token="your_sas_token"
            )
            
            list_options = AzListOptions(
                output_type="json",
                properties="LastModifiedTime;BlobType;ContentLength"
            )
            
            result = az_client.list_remote_location(remote_location, list_options)
        """
        return self._list(location=location, list_options=list_options)

    ####################################################################
    # Remove Data
    ####################################################################

    def remove_from_remote_location(
        self,
        location: AzRemoteSASLocation,
        remove_options: AzRemoveOptions,
    ) -> AzRemoveJobInfo:
        """
        Removes files and directories from a remote Azure Storage location.
        
        Args:
            location: The remote Azure storage location to remove from
            remove_options: Options for the remove operation (recursive, patterns, etc.)
            
        Returns:
            AzRemoveJobInfo containing the remove results
            
        Example:
            remote_location = AzRemoteSASLocation(
                storage_account="mystorageaccount",
                container="mycontainer", 
                path="myfolder/",
                sas_token="your_sas_token"
            )
            
            remove_options = AzRemoveOptions(
                recursive=True,
                exclude_pattern="*.log"
            )
            
            result = az_client.remove_from_remote_location(remote_location, remove_options)
        """
        return self._remove(location=location, remove_options=remove_options)

    def remove_single_blob(
        self,
        location: AzRemoteSASLocation,
        remove_options: AzRemoveOptions,
    ) -> AzRemoveJobInfo:
        """
        Removes a single blob from a remote Azure Storage location.
        
        Args:
            location: The remote Azure storage location of the blob to remove
            remove_options: Options for the remove operation
            
        Returns:
            AzRemoveJobInfo containing the remove results
            
        Example:
            remote_location = AzRemoteSASLocation(
                storage_account="mystorageaccount",
                container="mycontainer", 
                path="myfile.txt",
                sas_token="your_sas_token"
            )
            
            remove_options = AzRemoveOptions()
            
            result = az_client.remove_single_blob(remote_location, remove_options)
        """
        return self._remove(location=location, remove_options=remove_options)

    def remove_directory_recursive(
        self,
        location: AzRemoteSASLocation,
        remove_options: AzRemoveOptions,
    ) -> AzRemoveJobInfo:
        """
        Removes a directory and all its contents recursively from a remote Azure Storage location.
        
        Args:
            location: The remote Azure storage location of the directory to remove
            remove_options: Options for the remove operation (recursive will be set to True)
            
        Returns:
            AzRemoveJobInfo containing the remove results
            
        Example:
            remote_location = AzRemoteSASLocation(
                storage_account="mystorageaccount",
                container="mycontainer", 
                path="myfolder/",
                sas_token="your_sas_token"
            )
            
            remove_options = AzRemoveOptions(
                include_pattern="*.tmp"
            )
            
            result = az_client.remove_directory_recursive(remote_location, remove_options)
        """
        # Ensure recursive is set to True for directory removal
        remove_options.recursive = True
        return self._remove(location=location, remove_options=remove_options)
