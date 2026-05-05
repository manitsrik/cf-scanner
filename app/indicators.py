import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, MACD


def add_indicators(candles: pd.DataFrame) -> pd.DataFrame:
    df = candles.copy()
    df["ema_9"] = EMAIndicator(close=df["close"], window=9).ema_indicator()
    df["ema_21"] = EMAIndicator(close=df["close"], window=21).ema_indicator()
    df["ema_200"] = EMAIndicator(close=df["close"], window=200).ema_indicator()
    df["rsi_14"] = RSIIndicator(close=df["close"], window=14).rsi()
    macd = MACD(close=df["close"], window_slow=26, window_fast=12, window_sign=9)
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_diff"] = macd.macd_diff()
    df["volume_avg_20"] = df["volume"].rolling(window=20).mean()
    return df
