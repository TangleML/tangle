from __future__ import annotations

from typing import Annotated

import pydantic

NonEmptyStr = Annotated[str, pydantic.StringConstraints(min_length=1)]


class _BaseModel(pydantic.BaseModel):
    model_config = {"extra": "forbid"}


# --- Leaf argument models ---


class KeyExists(_BaseModel):
    key: NonEmptyStr


class ValueContains(_BaseModel):
    key: NonEmptyStr
    value_substring: NonEmptyStr


class ValueIn(_BaseModel):
    key: NonEmptyStr
    values: list[NonEmptyStr] = pydantic.Field(min_length=1)


class ValueEquals(_BaseModel):
    key: NonEmptyStr
    value: NonEmptyStr


class TimeRange(_BaseModel):
    """AwareDatetime requires timezone info (e.g. "2024-01-01T00:00:00Z").
    Naive datetimes like "2024-01-01T00:00:00" are rejected, preventing
    ambiguous timestamps that could silently resolve to the wrong timezone."""

    key: NonEmptyStr
    start_time: pydantic.AwareDatetime
    end_time: pydantic.AwareDatetime | None = None


# --- Predicate wrapper models (one field each) ---


class KeyExistsPredicate(_BaseModel):
    key_exists: KeyExists


class ValueContainsPredicate(_BaseModel):
    value_contains: ValueContains


class ValueInPredicate(_BaseModel):
    value_in: ValueIn


class ValueEqualsPredicate(_BaseModel):
    value_equals: ValueEquals


class TimeRangePredicate(_BaseModel):
    time_range: TimeRange


LeafPredicate = (
    KeyExistsPredicate
    | ValueContainsPredicate
    | ValueInPredicate
    | ValueEqualsPredicate
    | TimeRangePredicate
)


class NotPredicate(_BaseModel):
    not_: LeafPredicate = pydantic.Field(alias="not")


class AndPredicate(_BaseModel):
    and_: list["Predicate"] = pydantic.Field(alias="and", min_length=1)


class OrPredicate(_BaseModel):
    or_: list["Predicate"] = pydantic.Field(alias="or", min_length=1)


Predicate = (
    KeyExistsPredicate
    | ValueContainsPredicate
    | ValueInPredicate
    | ValueEqualsPredicate
    | TimeRangePredicate
    | NotPredicate
    | AndPredicate
    | OrPredicate
)

# Resolve forward reference to "Predicate" in recursive and/or models
AndPredicate.model_rebuild()
OrPredicate.model_rebuild()


class FilterQuery(_BaseModel):
    """Root: must be exactly one of {"and": [...]} or {"or": [...]}."""

    and_: list[Predicate] | None = pydantic.Field(None, alias="and", min_length=1)
    or_: list[Predicate] | None = pydantic.Field(None, alias="or", min_length=1)

    @pydantic.model_validator(mode="after")
    def _exactly_one_root_operator(self) -> FilterQuery:
        has_and = self.and_ is not None
        has_or = self.or_ is not None
        if has_and == has_or:
            raise ValueError("FilterQuery root must have exactly one of 'and' or 'or'.")
        return self
