from fastapi import HTTPException, status


class ConflictException(HTTPException):
    """
    Exception raised when a conflict occurs (HTTP 409).
    """
    def __init__(self, detail: str):
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail,
        )


class NotFoundException(HTTPException):
    """
    Exception raised when a resource is not found (HTTP 404).
    """
    def __init__(self, detail: str):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        )
