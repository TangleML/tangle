"""Unit tests for triggers.db_models.

Named test_trigger_db_models (not test_db_models) so the module basename stays unique
across the suite: the repo has no __init__.py in tests and pytest runs in the default
prepend import mode, which keys modules by basename, so a duplicate basename
(scheduling/pipelines/test_db_models.py) would collide on collection.
"""

import datetime
import itertools
from typing import Any

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend.triggers import db_models
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.utils import pipeline_templates

# Each auto-created pipeline needs its own file_path: `pipeline` is unique on
# (user_id, file_path), and a collision there would make a subscription test fail on the
# wrong table's constraint.
_pipeline_serial = itertools.count()


def _definition(name: str = "nightly-retrain") -> dict[str, Any]:
    """A minimal valid definition: the payload shape the API stores verbatim.

    No target key. The target lives in its own columns, not in this blob, so that a foreign
    key can enforce it — the definition is stored as received and never re-derived.
    """
    return {
        "name": name,
        "condition": {"op": "all", "children": [{"event": "dataset-ready"}]},
    }


def _add_subscription(
    session: orm.Session,
    *,
    name: str = "nightly-retrain",
    created_by: str = "test-owner",
    definition: dict[str, Any] | None = None,
    pipeline_id: str | None = None,
) -> str:
    """A committed subscription, with a pipeline created for it when none is supplied.

    Every subscription names a pipeline — the column is NOT NULL and a foreign key — so
    there is no such thing as a targetless one to write here.
    """
    definition = definition if definition is not None else _definition(name)
    if pipeline_id is None:
        pipeline_id = _add_pipeline(
            session, file_path=f"pipelines/auto-{next(_pipeline_serial)}.py"
        )
    # Both in one call, because that is what the API's write path does: the request model
    # validates the payload and then sets the column and the blob together. Nothing in
    # db_models re-derives the column.
    subscription = db_models.TriggerSubscription(
        name=definition.get("name", name),
        definition=definition,
        created_by=created_by,
        pipeline_task_spec_from_user_pipeline_id=pipeline_id,
    )
    session.add(subscription)
    session.commit()
    return subscription.id


def _add_pipeline(
    session: orm.Session,
    *,
    user_id: str = "test-owner",
    file_path: str = "pipelines/retrain.py",
) -> str:
    """A real row in `pipeline`, because the target columns are foreign keys.

    A subscription can no longer name a pipeline id out of thin air, so the tests below need
    the referenced row to exist first.
    """
    pipeline = user_pipeline_db_models.UserPipeline(
        user_id=user_id, file_path=file_path
    )
    session.add(pipeline)
    session.commit()
    return pipeline.id


def _add_version(
    session: orm.Session,
    *,
    pipeline_id: str,
    version_key: str = "a" * 64,
) -> str:
    version = user_pipeline_db_models.UserPipelineVersion(
        pipeline_id=pipeline_id,
        version_key=version_key,
        content_digest=version_key,
        root_pipeline_task={"name": "retrain"},
    )
    session.add(version)
    session.commit()
    return version.version_key


def _subscription_count(session: orm.Session) -> int | None:
    return session.scalar(
        sqlalchemy.select(sqlalchemy.func.count()).select_from(
            db_models.TriggerSubscription
        )
    )


class TestTheFence:
    def test_second_trigger_in_the_same_cycle_is_rejected(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # Two writers that both saw the condition satisfied are two independent commits, not
        # two rows batched into one — so commit the first, then attempt the second on its own
        # session and expect the DB to refuse it.
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session)

        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.TriggerHistory(subscription_id=subscription_id, cycle=0)
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            session.add(
                db_models.TriggerHistory(subscription_id=subscription_id, cycle=0)
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_the_next_cycle_is_a_new_slot(self, db_engine: sqlalchemy.Engine) -> None:
        # What the winner does after triggering: bump the cycle, which opens the next slot.
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session)
            session.add(
                db_models.TriggerHistory(subscription_id=subscription_id, cycle=0)
            )
            session.commit()

            session.add(
                db_models.TriggerHistory(subscription_id=subscription_id, cycle=1)
            )
            session.commit()

            assert (
                session.scalar(
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        db_models.TriggerHistory
                    )
                )
                == 2
            )

    def test_the_fence_is_scoped_to_one_subscription(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # Cycle counters are per subscription, so cycle 0 under a different subscription is
        # not a collision — otherwise one busy subscription would fence out every other.
        with orm.Session(bind=db_engine) as session:
            first = _add_subscription(session, name="first")
            second = _add_subscription(session, name="second")

            session.add(db_models.TriggerHistory(subscription_id=first, cycle=0))
            session.add(db_models.TriggerHistory(subscription_id=second, cycle=0))
            session.commit()

            assert (
                session.scalar(
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        db_models.TriggerHistory
                    )
                )
                == 2
            )


class TestDefaults:
    def test_a_new_subscription_starts_at_cycle_zero_and_enabled(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session)

        with orm.Session(bind=db_engine) as session:
            subscription = session.get(db_models.TriggerSubscription, subscription_id)
            assert subscription is not None
            assert subscription.cycle == 0
            assert subscription.enabled is True
            assert subscription.extra_data is None
            assert subscription.created_at is not None

    def test_an_unfilled_event_state_is_all_nulls(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The row exists as soon as the subscription declares the event; the NULLs are what
        # "has not arrived yet" looks like.
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session)
            session.add(
                db_models.TriggerEventState(
                    subscription_id=subscription_id, event_name="dataset-ready"
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            state = session.get(
                db_models.TriggerEventState, (subscription_id, "dataset-ready")
            )
            assert state is not None
            assert state.expire_seconds is None
            assert state.last_emission_event_id is None
            assert state.filled_at is None
            assert state.expires_at is None

    def test_expiry_round_trips_as_an_aware_utc_datetime(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # Freshness is decided in SQL against expires_at, so the column has to come back as
        # an aware UTC timestamp rather than a naive one.
        filled_at = datetime.datetime(2026, 3, 1, 12, 0, tzinfo=datetime.timezone.utc)
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session)
            session.add(
                db_models.TriggerEventState(
                    subscription_id=subscription_id,
                    event_name="dataset-ready",
                    expire_seconds=3600,
                    filled_at=filled_at,
                    expires_at=filled_at + datetime.timedelta(seconds=3600),
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            state = session.get(
                db_models.TriggerEventState, (subscription_id, "dataset-ready")
            )
            assert state is not None
            assert state.filled_at == filled_at
            assert state.expires_at == filled_at + datetime.timedelta(seconds=3600)


class TestCreatedBy:
    def test_a_subscription_without_an_owner_is_rejected(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The NOT NULL, exercised rather than inspected: the database refuses the row. The
        # target is supplied so that this test fails on the owner and nothing else.
        with orm.Session(bind=db_engine) as session:
            pipeline_id = _add_pipeline(session, file_path="ownerless.py")
            session.add(
                db_models.TriggerSubscription(
                    name="ownerless",
                    definition=_definition("ownerless"),
                    created_by=None,  # type: ignore[arg-type]
                    pipeline_task_spec_from_user_pipeline_id=pipeline_id,
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()


class TestTheNaturalKey:
    """uq_trigger_subscription_created_by_name — the handle a caller addresses a subscription by.

    The point of the constraint is that a caller can use their own name instead of persisting a
    Tangle-assigned id, which only holds if the database refuses to let one creator have two
    subscriptions under one name.
    """

    def test_one_creator_cannot_reuse_a_name(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # Separate sessions, because two callers racing to claim a handle are two independent
        # commits — not two rows batched into one flush.
        with orm.Session(bind=db_engine) as session:
            _add_subscription(session, name="nightly", created_by="alice")

        with orm.Session(bind=db_engine) as session:
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                _add_subscription(session, name="nightly", created_by="alice")

    def test_two_creators_may_share_a_name(self, db_engine: sqlalchemy.Engine) -> None:
        # The half that makes the key usable: the namespace is per creator, so alice claiming
        # "nightly" does not spend the name for everyone else.
        with orm.Session(bind=db_engine) as session:
            _add_subscription(session, name="nightly", created_by="alice")
            _add_subscription(session, name="nightly", created_by="bob")

            assert _subscription_count(session) == 2

    def test_one_creator_may_hold_many_names(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The other half: the constraint is on the pair, so it does not limit a creator to one
        # subscription.
        with orm.Session(bind=db_engine) as session:
            _add_subscription(session, name="nightly", created_by="alice")
            _add_subscription(session, name="weekly", created_by="alice")

            assert _subscription_count(session) == 2

    def test_a_rename_onto_a_taken_name_is_rejected(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The case an INSERT-only reading of UNIQUE would miss. `name` is mutable, so without
        # this the constraint could be walked around in two steps: create under a free name,
        # then rename onto the taken one. UNIQUE is checked on UPDATE too, so it cannot.
        with orm.Session(bind=db_engine) as session:
            _add_subscription(session, name="nightly", created_by="alice")
            weekly_id = _add_subscription(session, name="weekly", created_by="alice")

        with orm.Session(bind=db_engine) as session:
            weekly = session.get(db_models.TriggerSubscription, weekly_id)
            assert weekly is not None
            weekly.name = "nightly"
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_a_freed_name_can_be_claimed_again(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # trigger_subscription has no deleted_at, so a delete frees the handle at once. Worth
        # pinning: user_pipelines soft-deletes, where the row keeps holding its slot, and this
        # constraint is modelled on that one.
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(
                session, name="nightly", created_by="alice"
            )

        with orm.Session(bind=db_engine) as session:
            subscription = session.get(db_models.TriggerSubscription, subscription_id)
            assert subscription is not None
            session.delete(subscription)
            session.commit()

        with orm.Session(bind=db_engine) as session:
            _add_subscription(session, name="nightly", created_by="alice")

            assert _subscription_count(session) == 1


class TestTheDefinitionBlob:
    def test_the_definition_is_stored_exactly_as_posted(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The blob is never rewritten, so an edit round-trips and the payload shape can
        # grow without a migration.
        posted = _definition()
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session, definition=posted)

        with orm.Session(bind=db_engine) as session:
            subscription = session.get(db_models.TriggerSubscription, subscription_id)
            assert subscription is not None
            assert subscription.definition == posted
            assert subscription.name == posted["name"]

    def test_an_enum_keyed_blob_is_stored_under_the_literal_keys(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The write paths key the blob with DefinitionKey members. They are strings, so JSON
        # takes them — this is the test that says what comes back out is a plain "name" and
        # "condition", readable by anything that never heard of the enum.
        posted = {
            db_models.DefinitionKey.NAME: "nightly-retrain",
            db_models.DefinitionKey.CONDITION: {"event": "dataset-ready"},
        }
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session, definition=posted)

        with orm.Session(bind=db_engine) as session:
            subscription = session.get(db_models.TriggerSubscription, subscription_id)
            assert subscription is not None
            assert set(subscription.definition) == {"name", "condition"}
            assert [type(key) for key in subscription.definition] == [str, str]
            assert subscription.definition["condition"] == {"event": "dataset-ready"}

    def test_an_unknown_key_survives_the_round_trip(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The point of storing the payload verbatim: a key this version does not know about
        # comes back untouched instead of being dropped.
        posted = _definition() | {"future_key": {"nested": [1, 2, 3]}}
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session, definition=posted)

        with orm.Session(bind=db_engine) as session:
            subscription = session.get(db_models.TriggerSubscription, subscription_id)
            assert subscription is not None
            assert subscription.definition == posted


class TestMatchedEvents:
    def test_a_history_row_records_why_it_triggered(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # The three keys have to survive intact: branch to locate the term, branch_events as
        # the durable answer, and the definition snapshot so branch stays readable after the
        # condition is edited.
        definition = _definition()
        matched_events = {
            "branch": "all[0].any[1]",
            "branch_events": ["dataset-ready", "model-ready"],
            "definition": definition,
        }
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(session, definition=definition)
            session.add(
                db_models.TriggerHistory(
                    subscription_id=subscription_id,
                    cycle=0,
                    matched_events=matched_events,
                    triggered_by={"emission_event_ids": ["em-1", "em-2"]},
                )
            )
            session.commit()

        with orm.Session(bind=db_engine) as session:
            history = session.scalar(sqlalchemy.select(db_models.TriggerHistory))
            assert history is not None
            assert history.matched_events == matched_events
            assert history.matched_events["definition"] == definition
            assert history.triggered_by == {"emission_event_ids": ["em-1", "em-2"]}


class TestCascade:
    def test_deleting_a_subscription_leaves_no_orphan_event_states(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=fk_db_engine) as session:
            subscription_id = _add_subscription(session)
            session.add(
                db_models.TriggerEventState(
                    subscription_id=subscription_id, event_name="dataset-ready"
                )
            )
            session.commit()

            session.execute(
                sqlalchemy.delete(db_models.TriggerSubscription).where(
                    db_models.TriggerSubscription.id == subscription_id
                )
            )
            session.commit()

            assert (
                session.scalar(
                    sqlalchemy.select(sqlalchemy.func.count()).select_from(
                        db_models.TriggerEventState
                    )
                )
                == 0
            )

    def test_history_survives_its_subscription(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        # Deleting a subscription keeps the record that runs were started, and the
        # matched_events snapshot is what keeps that record readable with nothing to look up.
        with orm.Session(bind=fk_db_engine) as session:
            subscription_id = _add_subscription(session)
            session.add(
                db_models.TriggerHistory(
                    subscription_id=subscription_id,
                    cycle=0,
                    matched_events={"branch": "all[0]", "branch_events": ["a"]},
                )
            )
            session.commit()

            session.execute(
                sqlalchemy.delete(db_models.TriggerSubscription).where(
                    db_models.TriggerSubscription.id == subscription_id
                )
            )
            session.commit()

            history = session.scalar(sqlalchemy.select(db_models.TriggerHistory))
            assert history is not None
            assert history.subscription_id == subscription_id
            assert history.matched_events == {
                "branch": "all[0]",
                "branch_events": ["a"],
            }


class TestThePipelineTarget:
    """The two pipeline_task_spec_from_user_pipeline_* columns, and the three rules on them.

    A subscription names *what to start* the same way scheduled_pipeline_run does: a required
    pipeline, plus an optional version key that pins it. Three rules hold the pair together,
    and each is exercised here rather than inspected, because a constraint that is only
    asserted to exist is not evidence that the database enforces it.

      NOT NULL on the pipeline id                    there is always something to start
      fk_trigger_subscription_user_pipeline_id       the pipeline must exist
      fk_trigger_subscription_user_pipeline_version  the pin must be a version *of it*
    """

    def test_omitting_the_pipeline_is_a_type_error(self) -> None:
        # The NOT NULL is felt before the database is: the model is a kw_only dataclass with
        # no default on this column, so leaving the target out cannot even be constructed.
        # This is the failure a caller actually hits, and it happens without a session.
        with pytest.raises(TypeError):
            db_models.TriggerSubscription(  # type: ignore[call-arg]
                name="no-target",
                definition=_definition("no-target"),
                created_by="test-owner",
            )

    def test_a_null_pipeline_is_rejected_by_the_database(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        # And the same rule again one layer down, for the writer that goes around the
        # dataclass — a raw INSERT, or a later UPDATE that clears the column.
        with orm.Session(bind=fk_db_engine) as session:
            session.add(
                db_models.TriggerSubscription(
                    name="null-target",
                    definition=_definition("null-target"),
                    created_by="test-owner",
                    pipeline_task_spec_from_user_pipeline_id=None,  # type: ignore[arg-type]
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_clearing_the_pipeline_on_an_existing_row_is_rejected(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        # The UPDATE path. A subscription cannot be un-targeted after the fact either.
        with orm.Session(bind=fk_db_engine) as session:
            subscription_id = _add_subscription(session)

            subscription = session.get(db_models.TriggerSubscription, subscription_id)
            assert subscription is not None
            subscription.pipeline_task_spec_from_user_pipeline_id = None  # type: ignore[assignment]
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_a_target_with_no_pin_tracks_the_pipeline(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        # The common case. A NULL version key is not "no version" — it is "whichever version
        # is current when the condition fires", which is why nothing persists a mode column.
        with orm.Session(bind=fk_db_engine) as session:
            pipeline_id = _add_pipeline(session)
            subscription = db_models.TriggerSubscription(
                name="tracks-current",
                definition=_definition("tracks-current"),
                created_by="test-owner",
                pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            )
            session.add(subscription)
            session.commit()

            assert subscription.pipeline_task_spec_from_user_pipeline_id == pipeline_id
            assert (
                subscription.pipeline_task_spec_from_user_pipeline_version_key is None
            )

    def test_a_pin_names_a_version_of_that_pipeline(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=fk_db_engine) as session:
            pipeline_id = _add_pipeline(session)
            version_key = _add_version(session, pipeline_id=pipeline_id)
            subscription = db_models.TriggerSubscription(
                name="pinned",
                definition=_definition("pinned"),
                created_by="test-owner",
                pipeline_task_spec_from_user_pipeline_id=pipeline_id,
                pipeline_task_spec_from_user_pipeline_version_key=version_key,
            )
            session.add(subscription)
            session.commit()

            assert subscription.pipeline_task_spec_from_user_pipeline_id == pipeline_id
            assert (
                subscription.pipeline_task_spec_from_user_pipeline_version_key
                == version_key
            )

    def test_a_pipeline_that_does_not_exist_is_rejected(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        # What the single-column foreign key is for. The composite one below sits this case
        # out: it is MATCH SIMPLE, so a NULL pin skips it entirely.
        with orm.Session(bind=fk_db_engine) as session:
            session.add(
                db_models.TriggerSubscription(
                    name="ghost-target",
                    definition=_definition("ghost-target"),
                    created_by="test-owner",
                    pipeline_task_spec_from_user_pipeline_id="no-such-pipeline",
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_a_pin_that_is_not_a_version_is_rejected(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(bind=fk_db_engine) as session:
            pipeline_id = _add_pipeline(session)
            session.add(
                db_models.TriggerSubscription(
                    name="ghost-pin",
                    definition=_definition("ghost-pin"),
                    created_by="test-owner",
                    pipeline_task_spec_from_user_pipeline_id=pipeline_id,
                    pipeline_task_spec_from_user_pipeline_version_key="b" * 64,
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_a_pin_borrowed_from_another_pipeline_is_rejected(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        # Why the version foreign key is composite rather than one per column. Both values
        # exist; the *pair* does not. A per-column check would let this row through.
        with orm.Session(bind=fk_db_engine) as session:
            ours = _add_pipeline(session, file_path="ours.py")
            theirs = _add_pipeline(session, file_path="theirs.py")
            their_version = _add_version(session, pipeline_id=theirs)

            session.add(
                db_models.TriggerSubscription(
                    name="borrowed-pin",
                    definition=_definition("borrowed-pin"),
                    created_by="test-owner",
                    pipeline_task_spec_from_user_pipeline_id=ours,
                    pipeline_task_spec_from_user_pipeline_version_key=their_version,
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_a_pin_without_a_pipeline_is_rejected(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        # The hole both foreign keys leave, closed by the NOT NULL rather than by a CHECK:
        # MATCH SIMPLE means a NULL in either column skips the composite constraint, so a pin
        # with nothing to pin to would otherwise be stored and read back as an unresolvable
        # target. Requiring the pipeline id removes the state instead of legislating against
        # it, which is why ck_trigger_subscription_pin_needs_pipeline no longer exists.
        with orm.Session(bind=fk_db_engine) as session:
            session.add(
                db_models.TriggerSubscription(
                    name="dangling-pin",
                    definition=_definition("dangling-pin"),
                    created_by="test-owner",
                    pipeline_task_spec_from_user_pipeline_id=None,  # type: ignore[arg-type]
                    pipeline_task_spec_from_user_pipeline_version_key="a" * 64,
                )
            )
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                session.commit()

    def test_the_widths_are_the_pipeline_tables_own(self) -> None:
        # The reason the model declares no length constants of its own: the id column takes
        # its width through the foreign key, so widening pipeline.id widens this column too
        # and the two cannot drift apart. The version key is not a foreign key on its own —
        # it is half of a composite one — so it names the shared constant instead.
        columns = db_models.TriggerSubscription.__table__.c
        pipeline_columns = user_pipeline_db_models.UserPipeline.__table__.c

        assert (
            columns["pipeline_task_spec_from_user_pipeline_id"].type.length
            == pipeline_columns["id"].type.length
        )
        assert (
            columns["pipeline_task_spec_from_user_pipeline_version_key"].type.length
            == user_pipeline_db_models.DIGEST_LENGTH
        )


class TestTemplatesInsideTheDefinitionBlob:
    """The subscription has no `settings` column; its envelope rides in `definition`.
    Same accessor, so a render site never learns which kind it is holding."""

    @pytest.mark.parametrize(
        "definition",
        [{"name": "n"}, {"name": "n", "pipeline_templates": {}}],
        ids=["key-absent", "empty-envelope"],
    )
    def test_an_absent_or_empty_envelope_reads_as_none(
        self, db_engine: sqlalchemy.Engine, definition: dict[str, Any]
    ) -> None:
        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(
                session, name="n", definition=definition
            )
            loaded = session.get(db_models.TriggerSubscription, subscription_id)

            assert loaded is not None
            assert (
                pipeline_templates.get_pipeline_templates(original=loaded.definition)
                == {}
            )

    def test_a_populated_envelope_round_trips(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        envelope = {"arguments": {"as_of": "{{ trigger_time | date }}"}}

        with orm.Session(bind=db_engine) as session:
            subscription_id = _add_subscription(
                session,
                name="n",
                definition=pipeline_templates.set_pipeline_templates(
                    original={"name": "n"}, updates=envelope
                ),
            )
            loaded = session.get(db_models.TriggerSubscription, subscription_id)

            assert loaded is not None
            assert (
                pipeline_templates.get_pipeline_templates(original=loaded.definition)
                == envelope
            )

    def test_writing_templates_leaves_the_other_definition_keys_alone(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """`definition` is NOT NULL and already carries the subscription's name and
        condition; the envelope is added beside them, never over them."""
        original = {"name": "n", "condition": {"all": []}}

        updated = pipeline_templates.set_pipeline_templates(
            original=original, updates={"arguments": {"as_of": "x"}}
        )

        assert updated["name"] == "n"
        assert updated["condition"] == {"all": []}
        assert original == {"name": "n", "condition": {"all": []}}
