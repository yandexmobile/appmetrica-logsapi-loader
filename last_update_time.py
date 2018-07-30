import logging

import settings
from run import setup_logging
from state import FileStateStorage

logger = logging.getLogger(__name__)


def main():
    setup_logging(debug=settings.DEBUG)
    state_storage = FileStateStorage(
        file_name=settings.STATE_FILE_PATH
    )
    time = state_storage.load().last_update_time
    number = time.timestamp() if time is not None else 0
    print(int(number))


if __name__ == '__main__':
    main()
