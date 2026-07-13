"""Errors raised by the orchestration core."""


class OrchestratorError(Exception):
    """Base class for expected orchestration failures."""


class SchemaError(OrchestratorError, ValueError):
    """The workflow document is invalid."""


class StateError(OrchestratorError):
    """A state transition or persistence operation is invalid."""


class CorruptJournalError(StateError):
    """The durable event journal cannot be safely replayed."""
