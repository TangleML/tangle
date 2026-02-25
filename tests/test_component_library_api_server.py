import pytest
import yaml
from sqlalchemy import orm

from cloud_pipelines_backend import component_library_api_server as components_api
from cloud_pipelines_backend import component_structures, database_ops, errors


def _make_component_spec(name: str):
    return component_structures.ComponentSpec(
        name=name,
        implementation=component_structures.ContainerImplementation(
            container=component_structures.ContainerSpec(image="python")
        ),
    )


def _make_component_text(name: str):
    component_spec = _make_component_spec(name=name)
    return yaml.safe_dump(component_spec.to_json_dict())


def _initialize_db_and_get_session_factory():
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def test_published_component_service():
    session_factory = _initialize_db_and_get_session_factory()
    published_component_service = components_api.PublishedComponentService()
    user_name = "user 1"
    component_name = "component 1"
    component_text = _make_component_text(component_name)
    component_ref = component_structures.ComponentReference(text=component_text)

    # Test publishing
    with session_factory() as session:
        published_component = published_component_service.publish(
            session=session, component_ref=component_ref, user_name=user_name
        )
        assert published_component.digest
        assert published_component.published_by == user_name
        assert published_component.name == component_name
        assert published_component.deprecated == False
        assert published_component.superseded_by == None

    # Test listing
    with session_factory() as session:
        all_published_components = published_component_service.list(
            session=session
        ).published_components
        assert len(all_published_components) == 1

    # Test component search by digest
    with session_factory() as session:
        assert (
            len(
                published_component_service.list(
                    session=session, digest=published_component.digest
                ).published_components
            )
            == 1
        )
    with session_factory() as session:
        assert (
            len(
                published_component_service.list(
                    session=session, digest="XXX"
                ).published_components
            )
            == 0
        )

    # Test component search by name substring
    with session_factory() as session:
        filtered_published_components_1 = published_component_service.list(
            session=session, name_substring="comp"
        ).published_components
        assert len(filtered_published_components_1) == 1
    with session_factory() as session:
        filtered_published_components_2 = published_component_service.list(
            session=session, name_substring="XXX"
        ).published_components
    assert len(filtered_published_components_2) == 0

    # Test updating
    with session_factory() as session:
        published_component_2 = published_component_service.update(
            session=session,
            digest=published_component.digest,
            user_name=user_name,
            deprecated=True,
        )
        assert published_component_2.deprecated == True
    with session_factory() as session, pytest.raises(errors.ItemNotFoundError):
        published_component_service.update(
            session=session,
            digest=published_component.digest,
            user_name="XXX",
            deprecated=True,
        )

    # Test listing deprecated components
    with session_factory() as session:
        all_published_components_2 = published_component_service.list(
            session=session
        ).published_components
        assert len(all_published_components_2) == 0
    with session_factory() as session:
        all_published_components_3 = published_component_service.list(
            session=session, include_deprecated=True
        ).published_components
        assert len(all_published_components_3) == 1


def test_component_library_service():
    session_factory = _initialize_db_and_get_session_factory()
    published_component_service = components_api.PublishedComponentService()
    user_service = components_api.UserService()
    component_library_service = components_api.ComponentLibraryService()
    default_library_publisher = "Admin user"

    with session_factory() as session:
        component_library_service._initialize_empty_default_library_if_missing(
            session=session, published_by=default_library_publisher
        )

    # Test: The list of libraries contains the built-in default library
    with session_factory() as session:
        libraries_1 = component_library_service.list(
            session=session
        ).component_libraries
        assert len(libraries_1) == 1
        assert libraries_1[0].id == components_api.DEFAULT_COMPONENT_LIBRARY_ID
        assert libraries_1[0].name == components_api.DEFAULT_COMPONENT_LIBRARY_NAME

    user_name = "user 1"
    component_name = "component 1"
    component_text = _make_component_text(component_name)
    component_url = "https://example.com/component.yaml"

    library_name = "Library 1"
    library_folder = components_api.ComponentLibraryFolder(
        name=library_name,
        folders=[
            components_api.ComponentLibraryFolder(
                name="Folder 1",
                components=[
                    component_structures.ComponentReference(
                        text=component_text,
                        url=component_url,
                    )
                ],
            )
        ],
    )

    # Test: Creating a new library
    with session_factory() as session:
        library_2 = component_library_service.create(
            session=session,
            library=components_api.ComponentLibrary(
                name=library_name,
                root_folder=library_folder,
            ),
            user_name=user_name,
        )
        assert library_2.id
        assert library_2.name == library_name
        assert library_2.hide_from_search == False
        assert library_2.component_count == 1
        assert library_2.published_by == user_name
        assert library_2.root_folder
        assert library_2.root_folder.folders
        assert len(library_2.root_folder.folders) == 1
        assert library_2.root_folder.folders[0].components
        assert len(library_2.root_folder.folders[0].components) == 1
        component_ref_2 = library_2.root_folder.folders[0].components[0]
        assert component_ref_2.digest
        assert component_ref_2.name == component_name
        assert component_ref_2.url == component_url
        # By default, the returned library does not include component text or spec attributes
        assert component_ref_2.text == None
        assert component_ref_2.spec == None

    # Test: Test `get()`, `include_component_texts`
    with session_factory() as session:
        library_3 = component_library_service.get(
            session=session, id=library_2.id, include_component_texts=True
        )
        assert library_3.root_folder.folders
        assert library_3.root_folder.folders[0].components
        component_ref_3 = library_3.root_folder.folders[0].components[0]
        assert component_ref_3.text == component_text

    # Test: Test that the library components got published
    with session_factory() as session:
        published_components_list_4 = published_component_service.list(
            session=session, name_substring=component_name
        ).published_components
        assert published_components_list_4
        published_component_4 = published_components_list_4[0]
        assert published_component_4.digest == component_ref_2.digest
        assert published_component_4.url == component_url
        assert published_component_4.published_by == user_name

    # Test: New library can be found in `list()` results
    with session_factory() as session:
        libraries_5 = component_library_service.list(
            session=session
        ).component_libraries
        assert len(libraries_5) == 2

    # Test: New library can be found by name
    with session_factory() as session:
        libraries_6 = component_library_service.list(
            session=session, name_substring=library_name
        ).component_libraries
        assert len(libraries_6) == 1

    # Test: Replacing the library
    library_name_7 = "Library 7"
    library_folder_7 = components_api.ComponentLibraryFolder(
        name=library_name,
        folders=[
            components_api.ComponentLibraryFolder(
                name="Folder 7",
                components=[
                    component_structures.ComponentReference(
                        text=component_text,
                        url=component_url,
                    )
                ],
            )
        ],
    )
    library_request_7 = components_api.ComponentLibrary(
        name=library_name_7,
        root_folder=library_folder_7,
    )
    with pytest.raises(errors.PermissionError), session_factory() as session:
        component_library_service.replace(
            session=session,
            id=library_2.id,
            library=library_request_7,
            user_name="XXX",
        )

    with session_factory() as session:
        library_7 = component_library_service.replace(
            session=session,
            id=library_2.id,
            library=library_request_7,
            user_name=user_name,
        )
        assert library_7.name == library_name_7

    # Test: Getting user library
    user_library_id = components_api._get_component_library_id_for_user_name(
        user_name=user_name
    )
    with session_factory() as session:
        library_8 = component_library_service.get(
            session=session, id=user_library_id
        )
        assert library_8.id == user_library_id
        assert library_8.hide_from_search == True
        assert user_name in library_8.name

    # Test: Replacing user library
    library_name_9 = "Library 9"
    library_request_9 = components_api.ComponentLibrary(
        name=library_name_9,
        root_folder=library_folder_7,
    )
    with pytest.raises(errors.PermissionError), session_factory() as session:
        component_library_service.replace(
            session=session,
            id=user_library_id,
            library=library_request_9,
            user_name="XXX",
        )
    with session_factory() as session:
        library_9 = component_library_service.replace(
            session=session,
            id=user_library_id,
            library=library_request_9,
            user_name=user_name,
        )
        assert library_9.name == library_name_9

    # Test: Getting user library pins
    with session_factory() as session:
        pins_10 = user_service.get_component_library_pins(
            session=session, user_name=user_name
        ).component_library_ids
        assert len(pins_10) == 2
        assert components_api.DEFAULT_COMPONENT_LIBRARY_ID in pins_10
        assert user_library_id in pins_10

    # Test: Setting user library pins
    pins_11 = pins_10 + [library_2.id]
    with session_factory() as session:
        user_service.set_component_library_pins(
            session=session, user_name=user_name, component_library_ids=pins_11
        )
    with session_factory() as session:
        pins_11b = user_service.get_component_library_pins(
            session=session, user_name=user_name
        ).component_library_ids
        assert pins_11b == pins_11


if __name__ == "__main__":
    pytest.main()
