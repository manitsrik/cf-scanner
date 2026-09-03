"""Shared signal quality scoring and Thai trader summary text.

Both the scanner (live detection) and the API layer (legacy rows loaded from
SQLite) score signals, so the formulas live here instead of being duplicated.
"""

from __future__ import annotations


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def trend_distance_pct(price: float, ema_200: float) -> float:
    if price <= 0 or ema_200 <= 0:
        return 0.0
    return abs(price - ema_200) / price * 100


def cross_gap_pct(price: float, ema_9: float, ema_21: float) -> float:
    if price <= 0 or ema_9 <= 0 or ema_21 <= 0:
        return 0.0
    return abs(ema_9 - ema_21) / price * 100


def rsi_quality_score(side: str, rsi: float) -> float:
    if side == "LONG":
        if 50 <= rsi <= 68:
            return 100
        if 45 <= rsi < 50:
            return 70
        if 68 < rsi <= 75:
            return 65
        return 35
    if 32 <= rsi <= 50:
        return 100
    if 50 < rsi <= 55:
        return 70
    if 25 <= rsi < 32:
        return 65
    return 35


def label_for_score(score: float) -> str:
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Strong"
    if score >= 55:
        return "Caution"
    return "Weak"


def signal_quality(
    signal_type: str,
    price: float,
    ema_9: float,
    ema_21: float,
    ema_200: float,
    rsi: float,
    volume_ratio: float,
) -> dict:
    distance_pct = trend_distance_pct(price, ema_200)
    gap_pct = cross_gap_pct(price, ema_9, ema_21)
    trend_score = clamp(distance_pct / 1.2 * 100, 35, 100)
    cross_score = clamp(gap_pct / 0.35 * 100, 35, 100)
    volume_score = clamp(volume_ratio / 1.8 * 100, 35, 100)
    rsi_score = rsi_quality_score(signal_type, rsi)
    score = clamp(
        trend_score * 0.28 + cross_score * 0.2 + rsi_score * 0.24 + volume_score * 0.28,
        0,
        100,
    )
    return {
        "score": score,
        "label": label_for_score(score),
        "reasons": [
            f"Trend distance {distance_pct:.2f}% from EMA200",
            f"EMA cross gap {gap_pct:.3f}%",
            f"RSI quality {rsi_score:.0f}/100",
            f"Volume strength {volume_ratio:.2f}x avg20",
        ],
    }


def trader_summary(
    signal_type: str,
    quality_label: str | None,
    quality_score: float | None,
    rsi: float,
    volume_ratio: float,
    trade_plan: dict,
    backtest: dict,
    status: dict,
    news_context: dict | None = None,
) -> str:
    side_thai = "Long" if signal_type == "LONG" else "Short"
    score = float(quality_score or 0)
    risk_pct = float(trade_plan.get("risk_pct") or 0)
    win_rate = float(backtest.get("win_rate") or 0)
    sample_size = int(backtest.get("sample_size") or 0)
    state = status.get("state")
    relation = (news_context or {}).get("relation")

    strengths = []
    cautions = []

    if score >= 85:
        strengths.append(f"สัญญาณ {side_thai} แข็งแรงมากจาก quality {quality_label or ''} {score:.0f}")
    elif score >= 70:
        strengths.append(f"สัญญาณ {side_thai} ค่อนข้างดี quality {quality_label or ''} {score:.0f}")
    elif score >= 55:
        cautions.append("คุณภาพสัญญาณยังระดับต้องระวัง ควรรอ confirmation เพิ่ม")
    else:
        cautions.append("คุณภาพสัญญาณอ่อน ยังไม่เหมาะกับการรีบเข้าไม้")

    if volume_ratio >= 2:
        strengths.append(f"volume หนุนชัด {volume_ratio:.2f}x avg20")
    elif volume_ratio >= 1:
        strengths.append(f"volume ผ่านเกณฑ์ {volume_ratio:.2f}x avg20")

    if signal_type == "LONG" and rsi >= 68:
        cautions.append(f"RSI {rsi:.2f} เริ่มสูง ระวังไล่ราคา")
    if signal_type == "SHORT" and rsi <= 32:
        cautions.append(f"RSI {rsi:.2f} เริ่มต่ำ ระวังขายตามปลายทาง")

    if risk_pct >= 6:
        cautions.append(f"stop ค่อนข้างไกล {risk_pct:.2f}% ควรลดขนาดไม้")
    elif 0 < risk_pct <= 3:
        strengths.append(f"risk ต่อไม้คุมง่ายประมาณ {risk_pct:.2f}%")

    if sample_size >= 5:
        if win_rate >= 55:
            strengths.append(f"backtest ระยะสั้นหนุน win rate {win_rate:.0f}% จาก {sample_size} ตัวอย่าง")
        elif win_rate <= 40:
            cautions.append(f"backtest ระยะสั้นยังไม่สวย win rate {win_rate:.0f}% จาก {sample_size} ตัวอย่าง")
    else:
        cautions.append("backtest ยังมีตัวอย่างน้อย ใช้เป็นบริบทเท่านั้น")

    if relation == "conflicts":
        cautions.append("มีข่าวขัดกับทิศทางสัญญาณ ควรรอความผันผวนสงบหรือใช้ size เล็ก")
    elif relation == "supports":
        strengths.append("ข่าวล่าสุดหนุนทิศทางสัญญาณ")

    if state == "tp1":
        action = "สัญญาณนี้ถึง TP1 แล้ว ไม่ควรไล่เข้าใหม่ถ้าไม่มี setup ใหม่ ให้เน้นจัดการกำไรหรือรอย่อ"
    elif state == "stopped":
        action = "สัญญาณนี้โดน stop แล้ว ควรตัดออกจากแผนและรอสัญญาณใหม่"
    elif state == "expired":
        action = "สัญญาณหมดอายุแล้ว เพราะไม่ไปถึง TP1 หรือ SL ในกรอบเวลาที่กำหนด ควรรอ setup ใหม่"
    elif score >= 85 and risk_pct <= 6 and relation != "conflicts":
        action = "แผนที่เหมาะคือรอราคาอยู่ใน entry zone แล้วค่อยพิจารณาเข้า ไม่ไล่ราคา และวาง stop ตามแผน"
    elif score >= 70:
        action = "ใช้เป็น setup เฝ้าดูได้ แต่ควรรอแท่งยืนยันหรือจังหวะย่อเข้าใกล้ entry zone ก่อน"
    else:
        action = "ยังไม่ควรรีบเข้าไม้ ให้รอ confirmation เพิ่มหรือเลือก setup ที่คุณภาพสูงกว่า"

    strength_text = " จุดแข็ง: " + "; ".join(strengths[:3]) + "." if strengths else ""
    caution_text = " ข้อควรระวัง: " + "; ".join(cautions[:3]) + "." if cautions else ""
    return f"สรุปแบบเทรดเดอร์: {action}{strength_text}{caution_text}"
