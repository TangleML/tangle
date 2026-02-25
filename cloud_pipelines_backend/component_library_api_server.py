import copy
import dataclasses
import datetime
import hashlib
from typing import Any

import sqlalchemy as sql
import yaml
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import component_structures, errors


def calculate_digest_for_component_text(text: str) -> str:
    data = text.encode("utf-8")
    data = data.replace(b"\r\n", b"\n")  # Normalizing line endings
    digest = hashlib.sha256(data).hexdigest()
    return digest


MAX_COMPONENT_SIZE = 300_000


def load_component_spec_from_text_and_validate(
    text: str,
) -> component_structures.ComponentSpec:
    if len(text) > MAX_COMPONENT_SIZE:
        raise ValueError(f"Component size {len(text)} > {MAX_COMPONENT_SIZE=}.")
    component_dict = yaml.safe_load(text)
    return load_component_spec_from_dict_and_validate(component_dict)


def load_component_spec_from_dict_and_validate(
    component_dict: dict,
) -> component_structures.ComponentSpec:
    component_spec = component_structures.ComponentSpec.from_json_dict(component_dict)
    return validate_component_spec(component_spec)


def validate_component_spec(
    component_spec: component_structures.ComponentSpec,
) -> component_structures.ComponentSpec:
    # TODO: ! Validate the component !
    # component_structures._component_spec_post_init(component)
    return component_spec


# ComponentService


# DB Table
class ComponentRow(bts._TableBase):
    __tablename__ = "component"

    digest: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    # Component text is bigger than a normal string.
    # By default `str` is mapped to VARCHAR(255) so that it's still stored in the row.
    # sql.String(65535) is translated to VARCHAR(65535) which fails on MySQL:
    # "Column length too big for column 'value' (max = 16383); use BLOB or TEXT instead"
    text: orm.Mapped[str] = orm.mapped_column(sql.Text(), default=None)

    # The "spec" column (in JSON format) can be useful for querying.
    # The "text" column has YAML format and cannot be queried.
    spec: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # Do we need extra_data on Component?
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)


@dataclasses.dataclass(kw_only=True)
class ComponentResponse:
    digest: str
    text: str

    @staticmethod
    def from_db(component: ComponentRow) -> "ComponentResponse":
        return ComponentResponse(
            digest=component.digest,
            text=component.text,
        )


@dataclasses.dataclass(kw_only=True)
class ListComponentsResponse:
    components: list[ComponentResponse]


# This service probably should not be exposed directly to the API
class ComponentService:
    def _list(
        self, *, session: orm.Session, name_substring: str | None = None
    ) -> ListComponentsResponse:
        query = sql.select(ComponentRow)
        component_rows = session.scalars(query).all()
        return ListComponentsResponse(
            components=[
                ComponentResponse.from_db(component_row)
                for component_row in component_rows
            ]
        )

    def get(self, *, session: orm.Session, digest: str) -> ComponentResponse:
        component_row = session.get(ComponentRow, digest)
        if not component_row:
            raise errors.ItemNotFoundError(f"Component with {digest=} was not found.")
        return ComponentResponse.from_db(component_row)

    def add_from_text(
        self, *, session: orm.Session, component_text: str
    ) -> ComponentResponse:
        # TODO: !! validate component
        digest = calculate_digest_for_component_text(component_text)
        component_spec = load_component_spec_from_text_and_validate(component_text)
        session.rollback()
        component_row = session.get(ComponentRow, digest)
        if component_row:
            return ComponentResponse.from_db(component_row)

        component_row = ComponentRow(
            digest=digest,
            text=component_text,
            spec=component_spec.to_json_dict(),
        )
        session.add(component_row)
        response = ComponentResponse.from_db(component_row)
        session.commit()
        return response


# PublishedComponentService


# DB Table
class PublishedComponentRow(bts._TableBase):
    __tablename__ = "published_component"

    # There are queries that search by digest only.
    # But we do not need a separate index since digest is the 1st part fo the composite primary key.
    digest: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    published_by: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    deprecated: orm.Mapped[bool] = orm.mapped_column(default=False)
    superseded_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    url: orm.Mapped[str | None] = orm.mapped_column(default=None)
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    # Denormalized
    name: orm.Mapped[str | None] = orm.mapped_column(default=None)


@dataclasses.dataclass(kw_only=True)
class PublishedComponentResponse:
    digest: str
    published_by: str
    deprecated: bool = False
    superseded_by: str | None = None
    url: str | None = None
    name: str | None = None
    # text: str | None = None

    @staticmethod
    def from_db(published_component: PublishedComponentRow) -> "PublishedComponentResponse":
        return PublishedComponentResponse(
            digest=published_component.digest,
            published_by=published_component.published_by,
            deprecated=published_component.deprecated,
            superseded_by=published_component.superseded_by,
            url=published_component.url,
            name=published_component.name,
        )


@dataclasses.dataclass(kw_only=True)
class ListPublishedComponentsResponse:
    published_components: list[PublishedComponentResponse]


class PublishedComponentService:
    def list(
        self,
        *,
        session: orm.Session,
        include_deprecated: bool = False,
        name_substring: str | None = None,
        published_by_substring: str | None = None,
        digest: str | None = None,
    ) -> ListPublishedComponentsResponse:
        # TODO: Implement paging
        # TODO: Implement filtering/search
        # TODO: Implement visibility/access control
        query = sql.select(PublishedComponentRow)
        if digest:
            query = query.filter(PublishedComponentRow.digest == digest)
        if not include_deprecated:
            query = query.filter(PublishedComponentRow.deprecated.is_(False))
        if name_substring:
            query = query.filter(
                PublishedComponentRow.name.icontains(name_substring, autoescape=True)
            )
        if published_by_substring:
            query = query.filter(
                PublishedComponentRow.published_by.icontains(
                    published_by_substring, autoescape=True
                )
            )
        published_component_rows = session.scalars(query).all()
        return ListPublishedComponentsResponse(
            published_components=[
                PublishedComponentResponse.from_db(published_component_row)
                for published_component_row in published_component_rows
            ]
        )

    def publish(
        self,
        *,
        session: orm.Session,
        component_ref: component_structures.ComponentReference,
        user_name: str,
    ) -> PublishedComponentResponse:
        session.rollback()
        component_service = ComponentService()
        # Ideal: Use Text, else load text from URL, calculate Digest
        # Actual: Use Text else try to get text by Digest.
        # We currently don't load from URL for security reasons.

        component_text = component_ref.text
        digest = None
        if component_text:
            component_response = component_service.add_from_text(
                session=session, component_text=component_text
            )
            digest = component_response.digest
        else:
            # We currently don't load from URL for security reasons.
            if component_ref.digest:
                existing_component_row = component_service.get(
                    session=session, digest=component_ref.digest
                )
                if existing_component_row:
                    # Component with such digest already exists int he DB.
                    component_text = existing_component_row.text
                    digest = component_ref.digest
            if not (digest and component_text):
                raise ValueError(
                    "Component text is missing, cannot get component by digest (or digest is missing). Currently we cannot get component by URL for security reasons (you can get text from url yourself before publishing)."
                )

        component_spec = load_component_spec_from_text_and_validate(component_text)
        # Checking for existence
        key = (digest, user_name)
        published_component_row = session.get(PublishedComponentRow, key)
        if published_component_row:
            raise errors.ItemAlreadyExistsError(
                f"PublishedComponent with {key=} already exists. Use the `update` method to change it."
            )
        published_component = PublishedComponentRow(
            digest=digest,
            published_by=user_name,
            name=component_spec.name,
            url=component_ref.url,
        )

        session.add(published_component)
        response = PublishedComponentResponse.from_db(published_component)
        session.commit()
        return response

    def update(
        self,
        *,
        session: orm.Session,
        digest: str,
        user_name: str,
        deprecated: bool | None = None,
        superseded_by: str | None = None,
    ) -> PublishedComponentResponse:
        session.rollback()
        # TODO: Detect and prevent infinite `superseded_by` loops.
        key = (digest, user_name)
        published_component_row = session.get(PublishedComponentRow, key)
        if not published_component_row:
            raise errors.ItemNotFoundError(
                f"PublishedComponent with {key=} was not found."
            )
        if deprecated is not None:
            published_component_row.deprecated = deprecated
        if superseded_by is not None:
            published_component_row.superseded_by = superseded_by
        response = PublishedComponentResponse.from_db(published_component_row)
        session.commit()
        return response


# ComponentLibraryService

# Different kinds of IDs:
# * builtin:default
# * user:aaa@example.com
# * 01deadbeef
# I've considered using "~" instead of ":" to make the IDs more URL-safe.
# But the "@" symbol that is present in all e-mail addresses is not URL-safe anyway, so I decided to keep ":".

# Important lower-level design ideas
# User's component library pins:
# * Every user has default virtual list of pinned libraries that consists of the builtin default library ("builtin:default") and the user library ("user:<user_name>").
# * The DB entry for the user's component library pins is only added on first write.
# User's component library:
# * Every user has a virtual empty component library "user:<user_name>".
# * The DB entry for the user's component library is only added on first write.


DEFAULT_COMPONENT_LIBRARY_ID = "builtin:default"
DEFAULT_COMPONENT_LIBRARY_NAME = "Common components"
USER_LIBRARY_ID_PREFIX = "user:"


def _get_component_library_id_for_user_name(user_name: str):
    return USER_LIBRARY_ID_PREFIX + user_name


def _get_current_time() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


@dataclasses.dataclass(kw_only=True)
class ComponentLibraryFolder(component_structures._BaseModel):
    name: str  # Should folder name itself or its children?
    folders: "list[ComponentLibraryFolder] | None" = None
    # components: list[ComponentReference]
    components: list[component_structures.ComponentReference] | None = None
    annotations: dict[str, str] | None = None


class ComponentLibraryRow(bts._TableBase):
    __tablename__ = "component_library"

    # Id can be set manually
    id: orm.Mapped[str] = orm.mapped_column(
        primary_key=True, init=False, insert_default=bts.generate_unique_id
    )
    name: orm.Mapped[str]
    # root_folder: ComponentLibraryFolder # Store the whole structure as JSON
    #    name: str
    #    annotations: dict[str, str]
    #    folders: list[ComponentLibraryFolder]
    #    components: list[ComponentReference]
    root_folder: orm.Mapped[dict[str, Any]]
    created_at: orm.Mapped[datetime.datetime]
    updated_at: orm.Mapped[datetime.datetime]
    # created_by: str #?
    published_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # extra_owners can be implemented via extra_data
    extra_owners: orm.Mapped[list[str] | None] = orm.mapped_column(default=None)
    hide_from_search: orm.Mapped[bool] = orm.mapped_column(default=False)
    url: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # Annotations are provided by the library creator (unlike extra_data).
    # TODO: Switch to `dict[str, str] | None`
    annotations: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # Master branch of the file. Can be used for pinning seeded libraries. But there may be other solutions.
    # canonical_location: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # last_update_url: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # ?
    component_count: orm.Mapped[int] = orm.mapped_column(default=0)
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    @staticmethod
    def make_empty_user_library(user_name: str) -> "ComponentLibraryRow":
        current_time = _get_current_time()
        id = USER_LIBRARY_ID_PREFIX + user_name
        if id.startswith(USER_LIBRARY_ID_PREFIX):
            user_name = id.partition(":")[2]
        else:
            raise ValueError(
                "make_empty_user_library only supports user component libraries."
            )

        name = f"{user_name} components"

        component_library_row = ComponentLibraryRow(
            name=name,
            root_folder=ComponentLibraryFolder(name=name).to_json_dict(),
            created_at=current_time,
            updated_at=current_time,
            published_by=user_name,
            hide_from_search=True,
            component_count=0,
        )
        component_library_row.id = id
        return component_library_row


@dataclasses.dataclass(kw_only=True)
class ComponentLibrary:
    name: str
    root_folder: ComponentLibraryFolder
    annotations: dict[str, str] | None = None


@dataclasses.dataclass(kw_only=True)
class ComponentLibraryResponse:
    id: str
    name: str
    root_folder: ComponentLibraryFolder | None = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    published_by: str | None = None
    hide_from_search: bool = False
    annotations: dict[str, str] | None = None
    component_count: int = 0

    @staticmethod
    def from_db(
        component_library_row: ComponentLibraryRow,
        include_root_folder: bool = True,
    ) -> "ComponentLibraryResponse":
        return ComponentLibraryResponse(
            id=component_library_row.id,
            name=component_library_row.name,
            root_folder=(
                ComponentLibraryFolder.from_json_dict(component_library_row.root_folder)
                if include_root_folder
                else None
            ),
            created_at=component_library_row.created_at,
            updated_at=component_library_row.updated_at,
            published_by=component_library_row.published_by,
            hide_from_search=component_library_row.hide_from_search,
            annotations=component_library_row.annotations,
            component_count=component_library_row.component_count,
        )


@dataclasses.dataclass(kw_only=True)
class ListComponentLibrariesResponse:
    component_libraries: list[ComponentLibraryResponse]


class ComponentLibraryService:
    def list(
        self, *, session: orm.Session, name_substring: str | None = None
    ) -> ListComponentLibrariesResponse:
        # TODO: Implement filtering by user, URL
        # TODO: Implement visibility/access control
        query = sql.select(ComponentLibraryRow).filter(
            ComponentLibraryRow.hide_from_search.is_(False)
        )
        if name_substring:
            query = query.filter(
                ComponentLibraryRow.name.icontains(name_substring, autoescape=True)
            )
        session.rollback()
        component_library_rows = session.scalars(query).all()
        response = ListComponentLibrariesResponse(
            component_libraries=[
                ComponentLibraryResponse.from_db(
                    component_library_row, include_root_folder=False
                )
                for component_library_row in component_library_rows
            ]
        )
        return response

    def get(
        self,
        *,
        session: orm.Session,
        id: str,
        include_component_texts: bool = False,
    ) -> ComponentLibraryResponse:
        # TODO: Implement visibility/access control
        session.rollback()
        library_row = session.get(ComponentLibraryRow, id)
        if not library_row:
            # Handling empty user Library
            if id.startswith(USER_LIBRARY_ID_PREFIX):
                library_user_name = id.removeprefix(USER_LIBRARY_ID_PREFIX)
                library_row = ComponentLibraryRow.make_empty_user_library(
                    library_user_name
                )
            else:
                raise errors.ItemNotFoundError(
                    f"ComponentLibrary with {id=} was not found."
                )
        response = ComponentLibraryResponse.from_db(library_row)
        if include_component_texts:
            assert response.root_folder
            ComponentLibraryService._fill_component_texts_for_library_folder(
                session=session, library_folder=response.root_folder
            )
        return response

    @staticmethod
    def _fill_component_texts_for_library_folder(
        session: orm.Session, library_folder: ComponentLibraryFolder
    ):
        for component_ref in library_folder.components or []:
            component_row = session.get(ComponentRow, component_ref.digest)
            if not component_row:
                raise errors.ItemNotFoundError(
                    f"Component with {component_ref.digest=} was not found"
                )
            component_ref.text = component_row.text
        for child_folder in library_folder.folders or []:
            ComponentLibraryService._fill_component_texts_for_library_folder(
                session=session, library_folder=child_folder
            )

    def _prepare_new_library_and_publish_components(
        self,
        *,
        session: orm.Session,
        library: ComponentLibrary,
        user_name: str,
        publish_components: bool = True,
    ) -> ComponentLibraryRow:
        # Making copy, because we'll mutate the library (remove the text and spec)
        library = copy.deepcopy(library)
        # First let's publish all components in the library
        service = PublishedComponentService()
        session.rollback()
        component_count = 0
        for component_ref in ComponentLibraryService._recursively_iterate_over_all_component_refs_in_library_folder(
            library.root_folder
        ):
            if not component_ref.text:
                # TODO: Support publishing component from URL
                raise ValueError("Currently every library component must have text.")
            digest = calculate_digest_for_component_text(component_ref.text)
            if publish_components:
                try:
                    publish_response = service.publish(
                        session=session,
                        component_ref=component_ref,
                        user_name=user_name,
                    )
                    digest = publish_response.digest
                except errors.ItemAlreadyExistsError:
                    pass
            # Extract the component name:
            spec = component_ref.spec
            if not spec:
                spec = load_component_spec_from_text_and_validate(component_ref.text)
            component_name = spec.name
            # ComponentReference.name was intended for different purpose (relative path of the component directory),
            # but it has never really been used, so we can probably re-purpose it to hold the component name.
            component_ref.name = component_name
            # Remove text and spec so that the library is more compact.
            component_ref.digest = digest
            component_ref.text = None
            component_ref.spec = None
            component_count += 1

        current_time = _get_current_time()
        library_row = ComponentLibraryRow(
            name=library.name,
            root_folder=library.root_folder.to_json_dict(),
            created_at=current_time,
            updated_at=current_time,
            published_by=user_name,
            annotations=library.annotations,
            component_count=component_count,
        )
        return library_row

    def create(
        self,
        *,
        session: orm.Session,
        library: ComponentLibrary,
        user_name: str,
        hide_from_search: bool = False,
    ) -> ComponentLibraryResponse:
        session.rollback()
        component_library_row = self._prepare_new_library_and_publish_components(
            session=session,
            library=library,
            user_name=user_name,
            publish_components=not hide_from_search,
        )
        component_library_row.hide_from_search = hide_from_search
        session.add(component_library_row)
        session.commit()
        response = ComponentLibraryResponse.from_db(component_library_row)
        return response

    def _initialize_empty_default_library_if_missing(
        self, *, session: orm.Session, published_by: str
    ):
        session.rollback()
        existing_default_component_library = session.get(
            ComponentLibraryRow, DEFAULT_COMPONENT_LIBRARY_ID
        )
        if existing_default_component_library:
            return
        current_time = _get_current_time()
        root_folder = ComponentLibraryFolder(
            name=DEFAULT_COMPONENT_LIBRARY_NAME,
        )
        component_library_row = ComponentLibraryRow(
            name=DEFAULT_COMPONENT_LIBRARY_NAME,
            created_at=current_time,
            updated_at=current_time,
            published_by=published_by,
            root_folder=root_folder.to_json_dict(),
        )
        component_library_row.id = DEFAULT_COMPONENT_LIBRARY_ID
        session.add(component_library_row)
        session.commit()

    def replace(
        self,
        *,
        session: orm.Session,
        id: str,
        library: ComponentLibrary,
        user_name: str,
        hide_from_search: bool | None = None,
    ) -> ComponentLibraryResponse:
        session.rollback()
        component_library_row = session.get(ComponentLibraryRow, id)
        if not component_library_row:
            # Handling empty user Library
            if id.startswith(USER_LIBRARY_ID_PREFIX):
                library_user_name = id.removeprefix(USER_LIBRARY_ID_PREFIX)
                component_library_row = ComponentLibraryRow.make_empty_user_library(
                    library_user_name
                )
                session.add(component_library_row)
            else:
                raise errors.ItemNotFoundError(
                    f"ComponentLibrary with {id=} was not found."
                )
        owners = [component_library_row.published_by] + (
            component_library_row.extra_owners or []
        )
        if user_name not in owners:
            raise errors.PermissionError(
                f"User {user_name} does not have permission to update the component library {id}. {owners=}"
            )
        if hide_from_search is not None:
            component_library_row.hide_from_search = hide_from_search
        new_component_library_row = self._prepare_new_library_and_publish_components(
            session=session,
            library=library,
            user_name=user_name,
            publish_components=not component_library_row.hide_from_search,
        )
        # Should we auto-deprecate components that were removed from the library?

        component_library_row.root_folder = new_component_library_row.root_folder
        component_library_row.updated_at = new_component_library_row.updated_at
        component_library_row.name = new_component_library_row.name
        component_library_row.annotations = new_component_library_row.annotations
        component_library_row.component_count = (
            new_component_library_row.component_count
        )
        response = ComponentLibraryResponse.from_db(component_library_row)
        session.commit()
        return response

    @staticmethod
    def _recursively_iterate_over_all_component_refs_in_library_folder(
        library_folder: ComponentLibraryFolder,
    ):
        for child_folder in library_folder.folders or []:
            yield from ComponentLibraryService._recursively_iterate_over_all_component_refs_in_library_folder(
                child_folder
            )
        yield from library_folder.components or []


### UserService


class UserRow(bts._TableBase):
    __tablename__ = "user"

    # Unique user name
    id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    component_library_pin_ids: orm.Mapped[list[str] | None] = orm.mapped_column(
        default=None
    )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)


@dataclasses.dataclass(kw_only=True)
class UserComponentLibraryPinsResponse:
    component_library_ids: list[str]


class UserService:
    @staticmethod
    def _get_default_pinned_library_ids_for_user_name(user_name: str):
        return [
            _get_component_library_id_for_user_name(user_name),
            DEFAULT_COMPONENT_LIBRARY_ID,
        ]

    # ! User name must be set from Authentication and not exposed as an API parameter
    def get_component_library_pins(
        self, *, session: orm.Session, user_name: str
    ) -> UserComponentLibraryPinsResponse:
        session.rollback()
        user_row = session.get(UserRow, user_name)
        if user_row and user_row.component_library_pin_ids is not None:
            return UserComponentLibraryPinsResponse(
                component_library_ids=user_row.component_library_pin_ids
            )
        return UserComponentLibraryPinsResponse(
            component_library_ids=self._get_default_pinned_library_ids_for_user_name(
                user_name
            ),
        )

    def set_component_library_pins(
        self, *, session: orm.Session, user_name: str, component_library_ids: list[str]
    ):
        # Security note: User can try to pin libraries they don't have access to.
        # But they won't be able to request their contents (when we implement visibility/access control).
        # TODO: Verify that library IDs actually exist.
        session.rollback()
        user_row = session.get(UserRow, user_name)
        if (
            not user_row
            and component_library_ids
            == self._get_default_pinned_library_ids_for_user_name(user_name)
        ):
            return
        if not user_row:
            user_row = UserRow(id=user_name)
            session.add(user_row)
        user_row.component_library_pin_ids = component_library_ids
        session.commit()
