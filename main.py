# main.py
from data_pipeline import fetch_all_data
from utils.logger import setup_logger

if __name__ == "__main__":
    logger = setup_logger('main', 'bot.log')
    logger.info("Starting bot")
    fetch_all_data()
    logger.info("Data fetch complete")