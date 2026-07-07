import logging
import sys

from faststream._internal.logger.logging import get_logger

logger = get_logger(
    name="faststream_fastapi",
    log_level=logging.INFO,
    stream=sys.stderr,
    fmt="%(asctime)s %(levelname)8s - %(message)s",
)
