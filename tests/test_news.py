import pytest

from app.news import CryptoNewsService


@pytest.mark.parametrize(
    ("title", "sentiment", "impact", "market_wide"),
    [
        # Words that only contain a keyword must not trigger it.
        ("Bank of America expands crypto custody", "Neutral", "Normal", False),
        ("Bitcoin climbs for a second straight week", "Neutral", "Normal", False),
        ("Ethereum security upgrade goes live", "Neutral", "Normal", False),
        ("Solana hackathon winners announced", "Neutral", "Normal", False),
        # Real keywords, including inflected forms, still match.
        ("Exchange hacked as $40M drained", "Bearish", "High", False),
        ("SEC approves spot ether ETFs", "Bullish", "High", True),
        ("Fed holds rates steady", "Neutral", "High", True),
    ],
)
def test_news_keywords_match_whole_words(title, sentiment, impact, market_wide):
    item = {"title": title, "summary": ""}

    classified = CryptoNewsService._classify(item, [])

    assert classified["sentiment"] == sentiment
    assert classified["impact"] == impact
    assert CryptoNewsService._is_market_wide(item) is market_wide
