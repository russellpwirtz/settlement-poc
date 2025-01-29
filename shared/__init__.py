import logging
from dotenv import load_dotenv
import os

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a file handler
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.DEBUG)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and attach it to the handlers
formatter = logging.Formatter('%(asctime)s %(name)-15s %(levelname)-8s %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger

# Set this logger as the default for all modules
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-15s %(levelname)-8s %(message)s',
    handlers=[
        file_handler,
        console_handler
    ]
)

# Set specific log level for aiokafka's group_coordinator
logging.getLogger('aiokafka.consumer.group_coordinator').setLevel(logging.WARNING)

def load_env():
    load_dotenv()