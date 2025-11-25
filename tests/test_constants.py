from aqworker.constants import get_job_status_key, get_queue_name


def test_get_queue_name_prefixes():
    assert get_queue_name("emails") == "aqw:emails"


def test_get_job_status_key_prefixes():
    assert get_job_status_key("123") == "aqw:job:123"
