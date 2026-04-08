"""Tests for cloud_pipelines_backend.event_listeners."""

import threading
import typing

import pytest

from cloud_pipelines_backend import event_listeners


@pytest.fixture(autouse=True)
def reset_listeners() -> typing.Generator[None, None, None]:
    """Clear _listeners before and after every test."""
    event_listeners._listeners.clear()
    yield
    event_listeners._listeners.clear()


class _SampleEvent(event_listeners.Event):
    """Minimal Event subclass for testing."""


class _AnotherEvent(event_listeners.Event):
    """Second Event subclass to verify type-keyed dispatch."""


class TestSubscribeAndEmitSync:
    def test_callback_is_called_with_event(self) -> None:
        received: list[_SampleEvent] = []
        event = _SampleEvent()

        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=received.append,
            asynchronous=False,
        )
        event_listeners.emit(event=event)

        assert received == [event]

    def test_multiple_callbacks_all_called_in_order(self) -> None:
        calls: list[int] = []

        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=lambda _e: calls.append(1),
            asynchronous=False,
        )
        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=lambda _e: calls.append(2),
            asynchronous=False,
        )
        event_listeners.emit(event=_SampleEvent())

        assert calls == [1, 2]

    def test_emit_without_subscribers_is_noop(self) -> None:
        event_listeners.emit(event=_SampleEvent())

    def test_callbacks_only_fired_for_matching_event_type(self) -> None:
        calls: list[str] = []

        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=lambda _e: calls.append("sample"),
            asynchronous=False,
        )
        event_listeners.subscribe(
            event_type=_AnotherEvent,
            callback=lambda _e: calls.append("another"),
            asynchronous=False,
        )

        event_listeners.emit(event=_SampleEvent())

        assert calls == ["sample"]


class TestAsynchronousDispatch:
    def test_async_callback_runs_on_separate_thread(self) -> None:
        callback_thread_ident: list[int] = []
        done = threading.Event()

        def callback(_e: _SampleEvent) -> None:
            callback_thread_ident.append(threading.current_thread().ident)
            done.set()

        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=callback,
            asynchronous=True,
        )
        event_listeners.emit(event=_SampleEvent())

        assert done.wait(timeout=2.0), "async callback did not fire within 2 s"
        assert callback_thread_ident[0] != threading.main_thread().ident

    def test_asynchronous_defaults_to_true(self) -> None:
        callback_thread_ident: list[int] = []
        done = threading.Event()

        def callback(_e: _SampleEvent) -> None:
            callback_thread_ident.append(threading.current_thread().ident)
            done.set()

        event_listeners.subscribe(event_type=_SampleEvent, callback=callback)
        event_listeners.emit(event=_SampleEvent())

        assert done.wait(timeout=2.0), "default async callback did not fire within 2 s"
        assert callback_thread_ident[0] != threading.main_thread().ident

    def test_sync_callback_runs_on_calling_thread(self) -> None:
        callback_thread_ident: list[int] = []

        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=lambda _e: callback_thread_ident.append(
                threading.current_thread().ident
            ),
            asynchronous=False,
        )
        event_listeners.emit(event=_SampleEvent())

        assert callback_thread_ident == [threading.main_thread().ident]

    def test_mixed_sync_and_async_subscribers(self) -> None:
        sync_thread_ident: list[int] = []
        async_thread_ident: list[int] = []
        async_done = threading.Event()

        def sync_callback(_e: _SampleEvent) -> None:
            sync_thread_ident.append(threading.current_thread().ident)

        def async_callback(_e: _SampleEvent) -> None:
            async_thread_ident.append(threading.current_thread().ident)
            async_done.set()

        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=sync_callback,
            asynchronous=False,
        )
        event_listeners.subscribe(
            event_type=_SampleEvent,
            callback=async_callback,
            asynchronous=True,
        )

        event_listeners.emit(event=_SampleEvent())

        assert sync_thread_ident == [threading.main_thread().ident]
        assert async_done.wait(timeout=2.0), "async callback did not fire"
        assert async_thread_ident[0] != threading.main_thread().ident

    def test_exception_in_async_callback_does_not_propagate_to_caller(self) -> None:
        """A runtime exception raised inside an async callback must not reach emit()'s caller."""
        callback_ran = threading.Event()
        exception_raised = threading.Event()
        caught_exceptions: list[BaseException] = []

        original_excepthook = threading.excepthook

        def _capture(args: threading.ExceptHookArgs) -> None:
            caught_exceptions.append(args.exc_value)
            exception_raised.set()

        threading.excepthook = _capture
        try:
            def raising_callback(_event: _SampleEvent) -> None:
                callback_ran.set()
                raise RuntimeError("boom")

            event_listeners.subscribe(
                event_type=_SampleEvent,
                callback=raising_callback,
                asynchronous=True,
            )

            # emit() must return normally even though the callback will raise.
            event_listeners.emit(event=_SampleEvent())
            main_thread_continued = True

            assert callback_ran.wait(timeout=2.0), "async callback did not run"
            assert exception_raised.wait(timeout=2.0), "thread exception hook did not fire"
            assert main_thread_continued
            assert isinstance(caught_exceptions[0], RuntimeError)
        finally:
            threading.excepthook = original_excepthook
