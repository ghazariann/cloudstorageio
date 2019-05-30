import os
import logging
from datetime import datetime
import industry_parser


# Formatter
log_format = "[%(asctime)s] %(levelname)s - %(message)s --\t\t%(pathname)s:%(lineno)d"
formatter = logging.Formatter(log_format)

# File Handler
filename = "praxis_service_{}.log".format(datetime.today().strftime("%Y-%m-%d_%H-%M"))
file_dir = os.path.abspath(os.path.join(os.path.dirname(industry_parser.__file__), os.pardir))

if 'LOG_PATH' in os.environ:
    log_dir = os.environ['LOG_PATH']
    log_file_path = os.path.join(log_dir, filename)
else:
    log_file_path = os.path.join(file_dir, filename)

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Logger
logging.basicConfig(format=log_format)
logger = logging.getLogger('logger_info')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.info('logging to : "{}"'.format(log_file_path))
