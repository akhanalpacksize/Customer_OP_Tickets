import os
import json
import logging.config
from utils import create_folder_if_does_not_exist


def setup_logging(
        module_name,
        default_path=os.path.join(os.getcwd(), 'config', 'logging.json'),
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    path = default_path
    value = os.getenv(env_key, None)
    create_folder_if_does_not_exist('Logs')  # Ensure 'Logs' exists

    if value:
        path = value

    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)

        # Replace {module_name} in BOTH file handlers
        if 'info_file_handler' in config['handlers']:
            config['handlers']['info_file_handler']['filename'] = \
                config['handlers']['info_file_handler']['filename'].format(module_name=module_name)

        if 'error_file_handler' in config['handlers']:
            config['handlers']['error_file_handler']['filename'] = \
                config['handlers']['error_file_handler']['filename'].format(module_name=module_name)

        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
