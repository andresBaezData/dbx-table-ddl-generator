import logging

# Configuration
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

logger = logging.getLogger(__name__)