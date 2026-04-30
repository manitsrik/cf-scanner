import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator


def add_indicators(candles: pd.DataFrame) -> pd.DataFrame:
    df = candles.copy()
    df["ema_9"] = EMAIndicator(close=df["close"], window=9).ema_indicator()
    df["ema_21"] = EMAIndicator(close=df["close"], window=21).ema_indicator()
    df["ema_200"] = EMAIndicator(close=df["close"], window=200).ema_indicator()
    df["rsi_14"] = RSIIndicator(close=df["close"], window=14).rsi()
    df["volume_avg_20"] = df["volume"].rolling(window=20).mean()
    return df

