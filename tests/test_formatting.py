import pytest

from app.formatting import format_price


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (67123.45, "67123.45"),
        (0.15282, "0.15282"),
        (0.01283, "0.01283"),
        (0.00001234, "0.00001234"),
        (0.814, "0.814"),
        (100, "100"),
        (None, "-"),
        (float("nan"), "-"),
    ],
)
def test_format_price_keeps_significant_digits(value, expected):
    assert format_price(value) == expected
