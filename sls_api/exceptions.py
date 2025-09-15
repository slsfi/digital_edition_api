class CascadeUpdateError(Exception):
    """
    Exception raised for errors in cascading updates to related tables.

    Attributes:
        message (str): Explanation of the error.
    """
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

class DeleteError(Exception):
    def __init__(self, message: str, status=400):
        super().__init__(message)
        self.message = message
        self.status = status
