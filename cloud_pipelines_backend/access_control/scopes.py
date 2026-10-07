import dataclasses
import enum

from .. import errors


class BroadScope(str, enum.Enum):
    __hash__ = str.__hash__
    __str__ = str.__str__

    OPERATE = "operate"
    EDIT = "edit"
    MANAGE = "manage"


@dataclasses.dataclass(frozen=True, kw_only=True)
class ResourceType:
    name: str
    operate: frozenset[str]
    edit: frozenset[str]
    manage: frozenset[str]

    @property
    def fine_scopes(self) -> frozenset[str]:
        """Every fine scope this resource type defines."""
        return self.operate | self.edit | self.manage

    def expand(self, scope: str) -> frozenset[str]:
        """Fine scopes a grant stands for; a broad scope includes lower tiers, an unknown one nothing."""
        match scope:
            case BroadScope.OPERATE:
                return self.operate
            case BroadScope.EDIT:
                return self.operate | self.edit
            case BroadScope.MANAGE:
                return self.fine_scopes
        return frozenset({scope}) & self.fine_scopes

    def is_known(self, scope: str) -> bool:
        """True for a broad scope or one of this type's fine scopes."""
        return scope in set(BroadScope) or scope in self.fine_scopes


_resource_types: dict[str, ResourceType] = {}


def register_resource_type(
    *,
    name: str,
    operate: frozenset[str],
    edit: frozenset[str],
    manage: frozenset[str],
) -> ResourceType:
    """Declares a resource type's fine scopes per tier so grants on it can be checked."""
    resource_type = ResourceType(name=name, operate=operate, edit=edit, manage=manage)
    _resource_types[name] = resource_type
    return resource_type


def get_resource_type(name: str) -> ResourceType:
    """Looks up a registered resource type, raising ItemNotFoundError if none has that name."""
    if name not in _resource_types:
        raise errors.ItemNotFoundError(f"Unknown resource type {name}.")
    return _resource_types[name]
