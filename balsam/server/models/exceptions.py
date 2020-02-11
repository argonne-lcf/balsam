from rest_framework.exceptions import ValidationError


class InvalidStateError(ValidationError):
    pass
