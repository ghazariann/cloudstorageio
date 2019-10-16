import logging

# Formatter
log_format = "[%(asctime)s] %(levelname)s - %(message)s --\t\t%(pathname)s:%(lineno)d"

# Logger
logging.basicConfig(format=log_format)
logger = logging.getLogger('logger_info')
logger.setLevel(logging.INFO)
