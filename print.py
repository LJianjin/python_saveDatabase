import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    count = 0
    while True:
        logging.info('Hello world! {}'.format(count))
        count += 1
        time.sleep(1)