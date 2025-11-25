from aqworker.logger import setup_logger


def test_setup_logger_reuses_handlers():
    logger = setup_logger(name="coverage_logger", level="DEBUG")
    handler_ids = [id(handler) for handler in logger.handlers]

    logger_again = setup_logger(name="coverage_logger", level="WARNING")
    assert [id(handler) for handler in logger_again.handlers] == handler_ids

    logger.handlers.clear()


def test_setup_logger_accepts_custom_format():
    logger = setup_logger(
        name="format_logger", format_string="%(levelname)s:%(message)s"
    )
    formatter = logger.handlers[0].formatter
    assert formatter._fmt == "%(levelname)s:%(message)s"
    logger.handlers.clear()
