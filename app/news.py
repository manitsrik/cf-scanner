import asyncio
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx


@dataclass(frozen=True)
class NewsSource:
    name: str
    url: str


DEFAULT_NEWS_SOURCES = [
    NewsSource("Cointelegraph", "https://cointelegraph.com/rss"),
    NewsSource("Decrypt", "https://decrypt.co/feed"),
    NewsSource("The Block", "https://www.theblock.co/rss.xml"),
]

COIN_NAMES = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "ether", "eth"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
    "BNB": ["bnb", "binance coin", "binance"],
    "DOGE": ["dogecoin", "doge"],
    "ADA": ["cardano", "ada"],
    "TON": ["toncoin", "ton"],
    "SUI": ["sui"],
    "TRX": ["tron", "trx"],
    "AVAX": ["avalanche", "avax"],
    "LINK": ["chainlink", "link"],
    "LTC": ["litecoin", "ltc"],
    "BCH": ["bitcoin cash", "bch"],
    "DOT": ["polkadot", "dot"],
    "AAVE": ["aave"],
    "NEAR": ["near protocol", "near"],
    "UNI": ["uniswap", "uni"],
    "WLD": ["worldcoin", "wld"],
    "PEPE": ["pepe"],
    "SHIB": ["shiba inu", "shib"],
}

HIGH_IMPACT_KEYWORDS = {
    "approval",
    "approved",
    "ban",
    "cpi",
    "delist",
    "delisting",
    "etf",
    "exploit",
    "fed",
    "hack",
    "lawsuit",
    "regulation",
    "regulator",
    "reserve",
    "sec",
    "settlement",
    "stablecoin",
    "tariff",
}
POSITIVE_KEYWORDS = {
    "adoption",
    "approval",
    "approved",
    "bullish",
    "funding",
    "inflow",
    "launch",
    "partnership",
    "record",
    "rises",
    "surge",
}
NEGATIVE_KEYWORDS = {
    "ban",
    "bearish",
    "crackdown",
    "decline",
    "delist",
    "exploit",
    "falls",
    "hack",
    "lawsuit",
    "outflow",
    "plunge",
    "selloff",
}


class CryptoNewsService:
    def __init__(self, refresh_seconds: int = 600, item_limit: int = 12) -> None:
        self.refresh_seconds = refresh_seconds
        self.item_limit = item_limit
        self._cache_at: datetime | None = None
        self._cache: dict | None = None
        self._lock = asyncio.Lock()

    async def latest(self, symbols: list[str]) -> dict:
        now = datetime.now(timezone.utc)
        if self._cache and self._cache_at and (now - self._cache_at).total_seconds() < self.refresh_seconds:
            return self._filter_payload(self._cache, symbols)

        async with self._lock:
            now = datetime.now(timezone.utc)
            if self._cache and self._cache_at and (now - self._cache_at).total_seconds() < self.refresh_seconds:
                return self._filter_payload(self._cache, symbols)

            items = await self._fetch_all()
            payload = {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "refresh_seconds": self.refresh_seconds,
                "items": items,
            }
            self._cache = payload
            self._cache_at = datetime.now(timezone.utc)
            return self._filter_payload(payload, symbols)

    async def _fetch_all(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            results = await asyncio.gather(
                *(self._fetch_source(client, source) for source in DEFAULT_NEWS_SOURCES),
                return_exceptions=True,
            )

        items: list[dict] = []
        seen_links: set[str] = set()
        for result in results:
            if isinstance(result, Exception):
                continue
            for item in result:
                link = item.get("url") or item.get("title")
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
                items.append(item)

        items.sort(key=lambda item: item.get("published_at") or "", reverse=True)
        return items[: max(self.item_limit * 3, self.item_limit)]

    async def _fetch_source(self, client: httpx.AsyncClient, source: NewsSource) -> list[dict]:
        response = await client.get(source.url, headers={"User-Agent": "CF Scanner news reader/1.0"})
        response.raise_for_status()
        root = ET.fromstring(response.text)
        items = []
        for node in root.findall(".//item")[:20]:
            title = self._text(node, "title")
            if not title:
                continue
            description = self._strip_html(self._text(node, "description"))
            published_at = self._parse_date(self._text(node, "pubDate"))
            items.append(
                {
                    "source": source.name,
                    "title": title,
                    "url": self._text(node, "link"),
                    "published_at": published_at,
                    "summary": description[:220],
                }
            )
        return items

    def _filter_payload(self, payload: dict, symbols: list[str]) -> dict:
        active_symbols = symbols or []
        enriched = []
        for item in payload.get("items", []):
            matched_symbols = self._matched_symbols(item, active_symbols)
            if not matched_symbols and active_symbols:
                continue
            enriched.append({**item, **self._classify(item, matched_symbols), "symbols": matched_symbols})

        market_items = [item for item in payload.get("items", []) if self._is_market_wide(item)]
        for item in market_items:
            if any(existing.get("url") == item.get("url") for existing in enriched):
                continue
            enriched.append({**item, **self._classify(item, []), "symbols": []})

        enriched.sort(key=lambda item: (item.get("impact") == "High", item.get("published_at") or ""), reverse=True)
        visible_items = enriched[: self.item_limit]
        return {
            "updated_at": payload.get("updated_at"),
            "refresh_seconds": payload.get("refresh_seconds"),
            "items": visible_items,
            "summary": self._summary(visible_items),
        }

    @staticmethod
    def _matched_symbols(item: dict, symbols: list[str]) -> list[str]:
        text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
        matched = []
        for symbol in symbols:
            base = re.sub(r"^\d+", "", symbol.removesuffix("USDT"))
            terms = COIN_NAMES.get(base, [base.lower()])
            if any(CryptoNewsService._contains_term(text, term) for term in terms):
                matched.append(symbol)
        return matched

    @staticmethod
    def _contains_term(text: str, term: str) -> bool:
        return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text) is not None

    @staticmethod
    def _is_market_wide(item: dict) -> bool:
        text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
        terms = ["crypto market", "bitcoin etf", "fed", "sec", "stablecoin", "regulation", "cpi", "tariff"]
        return any(term in text for term in terms)

    @staticmethod
    def _classify(item: dict, matched_symbols: list[str]) -> dict:
        text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
        positive_hits = sorted(keyword for keyword in POSITIVE_KEYWORDS if keyword in text)
        negative_hits = sorted(keyword for keyword in NEGATIVE_KEYWORDS if keyword in text)
        high_hits = sorted(keyword for keyword in HIGH_IMPACT_KEYWORDS if keyword in text)

        sentiment = "Neutral"
        if len(positive_hits) > len(negative_hits):
            sentiment = "Bullish"
        elif len(negative_hits) > len(positive_hits):
            sentiment = "Bearish"

        impact = "High" if high_hits else "Normal"
        strength = CryptoNewsService._effect_strength(sentiment, len(positive_hits), len(negative_hits), impact)
        if matched_symbols:
            impact_reason = f"เกี่ยวกับ {', '.join(matched_symbols[:3])}"
        elif impact == "High":
            impact_reason = "ข่าวระดับตลาดที่อาจกระทบหลายเหรียญ"
        else:
            impact_reason = "ข่าวภาพรวมตลาด"

        targets = matched_symbols or ["Market"]
        coin_impacts = [
            {
                "symbol": symbol,
                "direction": sentiment,
                "strength": strength,
                "explanation": CryptoNewsService._thai_effect_explanation(
                    symbol=symbol,
                    sentiment=sentiment,
                    strength=strength,
                    positive_hits=positive_hits,
                    negative_hits=negative_hits,
                    high_hits=high_hits,
                ),
                "trade_note": CryptoNewsService._trade_note(sentiment, strength),
            }
            for symbol in targets[:4]
        ]
        return {
            "sentiment": sentiment,
            "impact": impact,
            "impact_reason": impact_reason,
            "effect_strength": strength,
            "coin_impacts": coin_impacts,
        }

    @staticmethod
    def _effect_strength(sentiment: str, positive: int, negative: int, impact: str) -> str:
        if sentiment == "Neutral":
            return "Watch"
        if impact == "High" or abs(positive - negative) >= 2:
            return "Strong"
        return "Mild"

    @staticmethod
    def _thai_effect_explanation(
        symbol: str,
        sentiment: str,
        strength: str,
        positive_hits: list[str],
        negative_hits: list[str],
        high_hits: list[str],
    ) -> str:
        target = "ตลาดรวม" if symbol == "Market" else symbol
        if sentiment == "Bullish":
            reason = CryptoNewsService._keyword_reason(positive_hits, high_hits)
            tone = "บวกแรง" if strength == "Strong" else "บวกอ่อน"
            return f"{target}: ประเมินเป็น{tone} เพราะข่าวมีคำ/ประเด็นเชิงบวก เช่น {reason}"
        if sentiment == "Bearish":
            reason = CryptoNewsService._keyword_reason(negative_hits, high_hits)
            tone = "ลบแรง" if strength == "Strong" else "ลบอ่อน"
            return f"{target}: ประเมินเป็น{tone} เพราะข่าวมีคำ/ประเด็นเสี่ยง เช่น {reason}"
        if high_hits:
            return f"{target}: ยังไม่ชัดว่าบวกหรือลบ แต่เป็นข่าวสำคัญเรื่อง {', '.join(high_hits[:3])} อาจทำให้ผันผวน"
        return f"{target}: ผลกระทบยังเป็นกลาง ใช้เป็นบริบทประกอบ ไม่ควรใช้เข้าเทรดเดี่ยว ๆ"

    @staticmethod
    def _keyword_reason(primary_hits: list[str], high_hits: list[str]) -> str:
        terms = primary_hits[:2] + [term for term in high_hits[:2] if term not in primary_hits[:2]]
        return ", ".join(terms[:3]) if terms else "sentiment ของพาดหัวข่าว"

    @staticmethod
    def _trade_note(sentiment: str, strength: str) -> str:
        if sentiment == "Bullish" and strength == "Strong":
            return "ถ้าเทคนิคให้ Long อยู่แล้ว ให้รอ candle confirm และเลี่ยงไล่ราคาหลังข่าว"
        if sentiment == "Bullish":
            return "ช่วยหนุนฝั่ง Long ได้เล็กน้อย แต่ยังต้องดู Best Setup และ RSI"
        if sentiment == "Bearish" and strength == "Strong":
            return "ถ้ามี Long signal ให้ลดขนาดไม้หรือรอความผันผวนสงบก่อน"
        if sentiment == "Bearish":
            return "เพิ่มความระวังฝั่ง Long และรอ confirmation ให้ชัดกว่าเดิม"
        return "ยังไม่ควรเปลี่ยนแผนเทรดจากข่าวนี้ ให้ใช้ technical signal เป็นหลัก"

    @staticmethod
    def _summary(items: list[dict]) -> str:
        if not items:
            return "ยังไม่พบข่าวที่ตรงกับ watchlist ในรอบนี้"
        high = sum(1 for item in items if item.get("impact") == "High")
        bearish = sum(1 for item in items if item.get("sentiment") == "Bearish")
        bullish = sum(1 for item in items if item.get("sentiment") == "Bullish")
        if high:
            return f"พบข่าวสำคัญ {high} ข่าว ควรตรวจผลกระทบก่อนเข้าไม้"
        if bullish > bearish:
            return "โทนข่าวเอนไปทางบวก แต่ยังใช้ signal เป็นตัวตัดสินหลัก"
        if bearish > bullish:
            return "โทนข่าวเอนไปทางลบ ระวังความผันผวนและลดขนาดไม้"
        return "ข่าวยังไม่เอนชัด ใช้เป็นบริบทประกอบแผนเทรด"

    @staticmethod
    def _text(node: ET.Element, tag: str) -> str:
        value = node.findtext(tag)
        return (value or "").strip()

    @staticmethod
    def _parse_date(value: str) -> str | None:
        if not value:
            return None
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _strip_html(value: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value)).strip()
