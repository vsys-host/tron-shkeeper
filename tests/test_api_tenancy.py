import pytest
from flask import Flask
from werkzeug.exceptions import BadRequest

from app.api import resolve_store_id


@pytest.fixture
def app():
    return Flask(__name__)


def test_missing_store_marker_defaults_to_store_one(app):
    with app.test_request_context("/", headers={}):
        assert resolve_store_id() == 1


def test_store_marker_selects_requested_store(app):
    with app.test_request_context("/", headers={"X-Store-ID": "7"}):
        assert resolve_store_id() == 7


@pytest.mark.parametrize("marker", ["", "abc", "0", "-1", "1.5"])
def test_invalid_store_marker_is_rejected(app, marker):
    with app.test_request_context("/", headers={"X-Store-ID": marker}):
        with pytest.raises(BadRequest):
            resolve_store_id()