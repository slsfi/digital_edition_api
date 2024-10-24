class CascadeUpdateError(Exception):
    """
    Exception raised for errors in cascading updates to related tables.

    Attributes:
        message (str): Explanation of the error.
    """
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
