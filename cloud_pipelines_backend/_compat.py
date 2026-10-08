"""Compatibility definitions for supported Python versions."""

import enum
from datetime import timezone

UTC = timezone.utc

try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, enum.Enum):
        """String-valued enum with Python 3.11's string conversion semantics."""

        __str__ = str.__str__
        __format__ = str.__format__

        @staticmethod
        def _generate_next_value_(name, start, count, last_values):
            return name.lower()
