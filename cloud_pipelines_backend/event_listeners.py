import dataclasses
import threading
import typing

_CallbackEntry: typing.TypeAlias = tuple[typing.Callable[..., None], bool]


@dataclasses.dataclass(frozen=True, kw_only=True)
class Event:
    """Marker base class for all event types."""


_EventType = typing.TypeVar("_EventType", bound=Event)

_listeners: dict[type, list[_CallbackEntry]] = {}


def subscribe(
    *,
    event_type: type[_EventType],
    callback: typing.Callable[[_EventType], None],
    asynchronous: bool = True,
) -> None:
    """Subscribe callback to event_type. Called once at startup per consumer.

    Args:
        event_type: The event class to subscribe to.
        callback: Called with the event instance when an event of that type is emitted.
        asynchronous: When True (default), the callback is dispatched on a
            separate daemon thread so emit() returns immediately. When False,
            the callback is invoked synchronously on the calling thread.
    """
    _listeners.setdefault(event_type, []).append((callback, asynchronous))


def emit(
    *,
    event: _EventType,
) -> None:
    """Dispatch event to all callbacks subscribed to its type."""
    for callback, asynchronous in _listeners.get(type(event), []):
        if asynchronous:
            threading.Thread(target=callback, args=(event,), daemon=True).start()
        else:
            callback(event)
