"""Domain errors. The API layer maps these to HTTP; nothing else raises HTTPException."""


class DomainError(Exception):
    code = "internal_error"
    http_status = 500

    def __init__(self, message: str, details: list[dict] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or []


class ValidationError(DomainError):
    code = "validation_error"
    http_status = 422


class NotFoundError(DomainError):
    code = "not_found"
    http_status = 404


class PermissionDenied(DomainError):
    code = "permission_denied"
    http_status = 403


class ConflictError(DomainError):
    code = "conflict"
    http_status = 409


class IllegalTransition(ConflictError):
    code = "illegal_transition"


class QueuePaused(ConflictError):
    code = "queue_paused"


class IdempotencyKeyReuse(ValidationError):
    code = "idempotency_key_reuse"


class IdempotencyInProgress(ConflictError):
    code = "idempotency_in_progress"


class PermanentError(DomainError):
    """Raised by a handler to skip backoff and dead-letter immediately."""

    code = "permanent_handler_error"
