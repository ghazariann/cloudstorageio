import os
import logging
from datetime import datetime
import cloudstorageio


# Formatter
log_format = "[%(asctime)s] %(levelname)s - %(message)s --\t\t%(pathname)s:%(lineno)d"
formatter = logging.Formatter(log_format)

# File Handler
logger_file = "loggers"
filename = "cloudstorageio_{}.loggers".format(datetime.today().strftime("%Y-%m-%d_%H-%M"))
package_dir = os.path.dirname(cloudstorageio.__file__)
logger_file_dir = os.path.join(package_dir, logger_file)
file_dir = os.path.abspath(logger_file_dir)

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

