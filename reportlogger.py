from enum import Enum


class ReportLevel(Enum):
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4


class ReportLogger:
    def __init__(self, level=ReportLevel.DEBUG):
        self.level = level

    def log(self, level: ReportLevel, message: str):
        if level.value >= self.level.value:
            # Indentation corresponding to ReportLevel
            indent = "\t"
            print(f"{level.name}:{indent}{message}")

    def debug(self, message: str):
        self.log(ReportLevel.DEBUG, message)

    def info(self, message: str):
        self.log(ReportLevel.INFO, message)

    def warning(self, message: str):
        self.log(ReportLevel.WARN, message)

    def error(self, message: str):
        self.log(ReportLevel.ERROR, message)


# Example usage of the logger, set at DEBUG level by default
report_logger = ReportLogger(ReportLevel.INFO)
