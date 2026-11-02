import math

PRICE_SIGNIFICANT_DIGITS = 6


def format_price(value: object) -> str:
    """Format a price with enough precision for both BTC and sub-cent coins.

    Fixed decimals hide low-priced coins (0.01283 -> 0.0128) and ``.6g`` switches
    to scientific notation below 0.0001, so keep six significant digits instead.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    if number == 0:
        return "0"

    magnitude = math.floor(math.log10(abs(number)))
    decimals = max(2, PRICE_SIGNIFICANT_DIGITS - magnitude - 1)
    return f"{number:.{decimals}f}".rstrip("0").rstrip(".")
