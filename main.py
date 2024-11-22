from parse import get_config
from consumer import consumer
from logger_config import logger

if __name__ == "__main__":
    logger.info("Starting the application...")
    config = get_config()
    logger.info(f"Loaded config: {config}")

    consumer1 = consumer(config["source"], config["storageType"], config["destination"])
    consumer1.listen()