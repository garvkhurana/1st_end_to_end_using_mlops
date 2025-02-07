import sys
import logging

class CustomException(Exception):
    """Custom Exception class for handling errors in the ML pipeline"""
    def __init__(self, error_message, error_detail: sys):
        super().__init__(error_message)
        self.error_message = self.get_detailed_error_message(error_message, error_detail)

    def get_detailed_error_message(self, error_message, error_detail: sys):
        _, _, exc_tb = error_detail.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno
        return f"Error occurred in file: {file_name}, line: {line_number}, message: {error_message}"

    def __str__(self):
        return self.error_message

# Configure logging
logging.basicConfig(
    filename="error.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def log_exception(error):
    """Logs the exception to a file"""
    logging.error(str(error))


