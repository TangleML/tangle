import enum


class Key(str, enum.Enum):
    __hash__ = str.__hash__
    __str__ = str.__str__

    USERS = "users"
    PERMISSIONS = "permissions"
    HISTORY = "history"
    REVISION = "revision"
    CHANGED_BY = "changed_by"
    CHANGED_AT = "changed_at"
    CHANGES = "changes"
    FROM = "from"
    TO = "to"
