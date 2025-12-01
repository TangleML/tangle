import copy
import dataclasses
import logging
import pathlib
from typing import Optional

import huggingface_hub
from huggingface_hub import hf_api

from cloud_pipelines.orchestration.storage_providers import interfaces

_LOGGER = logging.getLogger(name=__name__)


# hf://(model|dataset|space)s/user/repo@branch/path


@dataclasses.dataclass
class HuggingFaceRepoUri(interfaces.DataUri):
    # uri: str
    repo_type: str  # model | dataset | space
    user: str
    repo: str
    path: str
    branch: str | None = None

    def join_path(self, relative_path: str) -> "HuggingFaceRepoUri":
        new_uri = copy.copy(self)
        new_uri.path = new_uri.path.rstrip("/") + "/" + relative_path
        return new_uri

    def __str__(self):
        return f"hf://{self.repo_type}s/{self.user}/{self.repo}{'@' + self.branch if self.branch else ''}/{self.path}"

    @classmethod
    def parse(cls, uri_string: str) -> "HuggingFaceRepoUri":
        # Validating the URI
        if not uri_string.startswith("hf://"):
            raise ValueError(
                f"HuggingFace URI must start with hf://, but got {uri_string}"
            )
        parts = uri_string.split("/", 5)
        repo_type = parts[2].rstrip("s")  # Making type singular
        user = parts[3]
        repo, _, branch = parts[4].partition("@")
        path = parts[5]
        if repo_type not in ("model", "dataset", "space"):
            raise ValueError(
                f"HuggingFace URI repo_type must be (model | dataset | space), but got {uri_string}"
            )
        if not user:
            raise ValueError(f"HuggingFace URI must have user, but got {uri_string}")
        if not repo:
            raise ValueError(f"HuggingFace URI must have repo, but got {uri_string}")
        if not path:
            raise ValueError(f"HuggingFace URI must have path, but got {uri_string}")

        return HuggingFaceRepoUri(
            repo_type=repo_type,
            user=user,
            repo=repo,
            branch=branch,
            path=path,
        )

    @property
    def repo_id(self):
        return f"{self.user}/{self.repo}"


HuggingFaceRepoUri._register_subclass("huggingface_repo_storage")


class HuggingFaceRepoStorageProvider(interfaces.StorageProvider):
    def __init__(self, client: Optional[huggingface_hub.HfApi] = None) -> None:
        self._client = client or huggingface_hub.HfApi()

    def make_uri(self, uri: str) -> interfaces.UriAccessor:
        return interfaces.UriAccessor(
            uri=HuggingFaceRepoUri.parse(uri),
            provider=self,
        )

    def parse_uri_get_accessor(self, uri_string: str) -> interfaces.UriAccessor:
        return interfaces.UriAccessor(
            uri=HuggingFaceRepoUri.parse(uri_string),
            provider=self,
        )

    def upload(self, source_path: str, destination_uri: HuggingFaceRepoUri):
        _LOGGER.debug(f"Uploading from {source_path} to {destination_uri}")
        if pathlib.Path(source_path).is_dir:
            self._client.upload_folder(
                repo_type=destination_uri.repo_type,
                repo_id=destination_uri.repo_id,
                path_in_repo=destination_uri.path,
                folder_path=source_path,
                commit_message=destination_uri.path,
            )
        else:
            self._client.upload_file(
                repo_type=destination_uri.repo_type,
                repo_id=destination_uri.repo_id,
                path_in_repo=destination_uri.path,
                path_or_fileobj=source_path,
                commit_message=destination_uri.path,
            )

    def download(self, source_uri: HuggingFaceRepoUri, destination_path: str):
        _LOGGER.debug(f"Downloading from {source_uri} to {destination_path}")
        cache_path = self._client.snapshot_download(
            repo_type=source_uri.repo_type,
            repo_id=source_uri.repo_id,
            allow_patterns=source_uri.path + "*",
        )
        cache_data_path = pathlib.Path(cache_path, source_uri.path).resolve()
        _LOGGER.debug(
            f"Downloaded data to from {source_uri} to cache ({cache_path}). The data should be in {cache_data_path}"
        )
        import shutil

        if cache_data_path.is_dir:
            shutil.copytree(cache_data_path, destination_path, dirs_exist_ok=True)
        else:
            pathlib.Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(cache_data_path, destination_path)

    def download_bytes(self, source_uri: HuggingFaceRepoUri) -> bytes:
        cache_path = self._client.hf_hub_download(
            repo_type=source_uri.repo_type,
            repo_id=source_uri.repo_id,
            filename=source_uri.path,
        )
        return pathlib.Path(cache_path).read_bytes()

    def exists(self, uri: HuggingFaceRepoUri) -> bool:
        repo_objects = self._client.get_paths_info(
            repo_type=uri.repo_type, repo_id=uri.repo_id, paths=[uri.path]
        )
        return len(repo_objects) > 0

    def calculate_data_hash(self, *, data: bytes) -> dict[str, str]:
        import hashlib

        header = f"blob {len(data)}\0".encode("utf-8")
        hasher = hashlib.sha1(header)
        hasher.update(data)
        return {"GitBlobHash": hasher.hexdigest()}

    def get_info(self, uri: HuggingFaceRepoUri) -> interfaces.DataInfo:
        repo_objects = self._client.get_paths_info(
            repo_type=uri.repo_type, repo_id=uri.repo_id, paths=[uri.path]
        )
        if not repo_objects:
            raise ValueError(f"Uri {uri} was not found.")
        if len(repo_objects) > 1:
            raise ValueError(
                f"Uri {uri} was found more than once. This cannot happen. {repo_objects}"
            )
        repo_object = repo_objects[0]

        if isinstance(repo_object, hf_api.RepoFile):
            return interfaces.DataInfo(
                is_dir=False,
                total_size=repo_object.size,
                hashes={"GitBlobHash": repo_object.blob_id},
            )
        elif isinstance(repo_object, hf_api.RepoFolder):
            # Calculating the total size of files in the folder
            child_repo_objects = huggingface_hub.list_repo_tree(
                repo_id=uri.repo_id,
                repo_type=uri.repo_type,
                path_in_repo=uri.path,
                recursive=True,
            )
            total_size = sum(
                child_repo_object.size
                for child_repo_object in child_repo_objects
                if isinstance(child_repo_object, hf_api.RepoFile)
            )
            return interfaces.DataInfo(
                is_dir=True,
                total_size=total_size,
                hashes={"GitTreeHash": repo_object.tree_id},
            )
        else:
            raise ValueError(
                f"Got repo object that is neither RepoFile nor RepoFolder: {repo_object}"
            )
