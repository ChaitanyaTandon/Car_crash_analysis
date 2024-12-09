import logging

class AppLogger:

    def __init__(self,log_file_name,log_file_path):
        self.log_file_name = log_file_name
        self.log_file_path = log_file_path
        self.log_file = self.log_file_path + self.log_file_name
        self.configure_logging()

    def configure_logging(self):
        logging.basicConfig(
            filename=self.log_file,
            encoding="utf-8",
            filemode="w",
            format="{asctime} - {levelname} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M",
            level=logging.INFO
        )
    
    def log_debug(self,message,exc_info=False):
        logging.debug(message,exc_info=exc_info)

    def log_info(self,message,exc_info=False):
        logging.info(message,exc_info=exc_info)

    def log_warning(self,message,exc_info=False):
        logging.warning(message,exc_info=exc_info)

    def log_error(self,message,exc_info=True):
        logging.error(message,exc_info=exc_info)

    def log_critical(self,message,exc_info=True):
        logging.critical(message,exc_info=exc_info)