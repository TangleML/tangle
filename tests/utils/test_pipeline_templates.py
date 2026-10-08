"""Unit tests for utils.pipeline_templates."""

import json
from typing import Any

import pytest

from cloud_pipelines_backend.utils import pipeline_templates


class TestGetCollapsesEveryWayOfSayingAbsent:
    @pytest.mark.parametrize(
        "original",
        [None, {}, {"other_feature": 1}, {"pipeline_templates": {}}],
        ids=["null", "empty-blob", "key-missing", "empty-envelope"],
    )
    def test_they_all_read_as_an_empty_dict(
        self, original: dict[str, Any] | None
    ) -> None:
        assert pipeline_templates.get_pipeline_templates(original=original) == {}

    def test_a_populated_envelope_is_returned_as_stored(self) -> None:
        envelope = {"arguments": {"as_of": "{{ schedule_time | date }}"}}

        assert (
            pipeline_templates.get_pipeline_templates(
                original={"pipeline_templates": envelope}
            )
            == envelope
        )


class TestSetReturnsANewBlob:
    def test_the_input_blob_is_not_mutated(self) -> None:
        """The caller assigns the result to a mapped column, and that assignment is what
        tells SQLAlchemy the JSON changed. Mutating in place would work only through
        `MutableDict` and would silently do nothing for a caller holding a plain dict.
        """
        original: dict[str, Any] = {"other_feature": 1}

        updated = pipeline_templates.set_pipeline_templates(
            original=original, updates={"arguments": {}}
        )

        assert original == {"other_feature": 1}
        assert updated is not original

    def test_it_works_from_a_null_blob(self) -> None:
        envelope = {"arguments": {"as_of": "x"}}

        assert pipeline_templates.set_pipeline_templates(
            original=None, updates=envelope
        ) == {"pipeline_templates": envelope}

    def test_other_keys_survive(self) -> None:
        updated = pipeline_templates.set_pipeline_templates(
            original={"other_feature": 1}, updates={"arguments": {"as_of": "x"}}
        )

        assert updated["other_feature"] == 1


class TestClearingRemovesTheKey:
    """Storing `{}` would mean a set-then-clear round trip left a blob that is not the one
    it started from, and an empty envelope that every reader must then collapse forever.
    """

    @pytest.mark.parametrize("empty", [{}, None], ids=["empty-dict", "none"])
    def test_an_empty_envelope_removes_the_key_rather_than_storing_it(
        self, empty: dict[str, Any] | None
    ) -> None:
        stored = {
            "pipeline_templates": {"arguments": {"as_of": "x"}},
            "other_feature": 1,
        }

        updated = pipeline_templates.set_pipeline_templates(
            original=stored, updates=empty or {}
        )

        assert "pipeline_templates" not in updated
        assert updated == {"other_feature": 1}

    def test_set_then_clear_restores_the_original_blob(self) -> None:
        original: dict[str, Any] = {"name": "n"}

        written = pipeline_templates.set_pipeline_templates(
            original=original, updates={"arguments": {"as_of": "x"}}
        )
        cleared = pipeline_templates.set_pipeline_templates(
            original=written, updates={}
        )

        assert cleared == original

    def test_clearing_a_blob_that_never_had_the_key_is_a_no_op(self) -> None:
        assert pipeline_templates.set_pipeline_templates(
            original={"name": "n"}, updates={}
        ) == {"name": "n"}


class TestTheStoredKeyIsThePublicWireName:
    """The key is what is already on disk and what the API field is called, so it is
    asserted as a literal rather than read back off the enum."""

    def test_the_envelope_is_stored_under_pipeline_templates(self) -> None:
        updated = pipeline_templates.set_pipeline_templates(
            original=None, updates={"arguments": {}}
        )

        assert list(updated) == ["pipeline_templates"]

    def test_the_written_key_is_a_plain_str_not_an_enum_member(self) -> None:
        """Every row loaded from MySQL arrives through a JSON decoder, so its key is a plain
        `str`. A blob built here must be indistinguishable from that one, or the same blob
        compares, reprs and serialises two different ways depending on where it came from.
        """
        updated = pipeline_templates.set_pipeline_templates(
            original=None, updates={"arguments": {}}
        )

        assert type(next(iter(updated))) is str

    def test_it_survives_a_json_round_trip(self) -> None:
        envelope = {"arguments": {"as_of": "{{ schedule_time | date }}"}}

        written = pipeline_templates.set_pipeline_templates(
            original=None, updates=envelope
        )
        decoded = json.loads(json.dumps(written))

        assert decoded == written
        assert pipeline_templates.get_pipeline_templates(original=decoded) == envelope
