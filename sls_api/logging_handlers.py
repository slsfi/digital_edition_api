import logging

class WarningErrorFlagHandler(logging.Handler):
    """
    A logging.Handler that flips boolean flags when warnings/errors occur.

    Attaching this handler to a logger sets:
    - had_warning: True if at least one WARNING was logged
      (excludes ERROR/CRITICAL).
    - had_error: True if at least one ERROR or CRITICAL was logged.

    Only WARNING and above are processed; lower levels are ignored.
    """
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.had_warning = False
        self.had_error = False

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self.had_error = True
        elif record.levelno == logging.WARNING:
            self.had_warning = True
