#!/usr/bin/env python3
"""
投资分析 Skill — 技术分析模块
计算：MA/MACD/RSI/支撑压力位/缺口/趋势判断
所有源码透明可审计。
"""

import argparse
import json
import sys

try:
    import numpy as np
except ImportError:
    print("错误：需要 numpy 库。请运行: pip install numpy")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("错误：需要 pandas 库。请运行: pip install pandas")
    sys.exit(1)


def load_data(filepath: str) -> dict:
    """加载 fetch_market_data.py 输出的 JSON"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("success"):
        print(f"错误：数据文件标记为失败 — {data.get('error', '未知')}")
        sys.exit(1)

    return data


def build_dataframe(history: list) -> pd.DataFrame:
    """将历史数据构建为 DataFrame"""
    df = pd.DataFrame(history)

    # 确保有必要的列
    required = ["date", "close"]
    for col in required:
        if col not in df.columns:
            print(f"错误：历史数据缺少 '{col}' 列")
            sys.exit(1)

    # 类型转换
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 如果缺少 OHLC 中的某些列，用 close 填充
    if "open" not in df.columns:
        df["open"] = df["close"]
    if "high" not in df.columns:
        df["high"] = df["close"]
    if "low" not in df.columns:
        df["low"] = df["close"]
    if "volume" not in df.columns:
        df["volume"] = 0

    return df


# ============================================================
#  技术指标计算
# ============================================================

def calc_ma(df: pd.DataFrame) -> dict:
    """计算移动平均线"""
    result = {}
    for period in [5, 10, 20, 60]:
        col = f"MA{period}"
        if len(df) >= period:
            df[col] = df["close"].rolling(window=period).mean()
            result[col] = round(df[col].iloc[-1], 4)
        else:
            result[col] = None

    # 均线排列判断
    ma_values = [result.get(f"MA{p}") for p in [5, 10, 20] if result.get(f"MA{p}") is not None]
    if len(ma_values) >= 3:
        if ma_values == sorted(ma_values, reverse=True):
            result["arrangement"] = "多头排列（短期均线在上，看涨信号）"
        elif ma_values == sorted(ma_values):
            result["arrangement"] = "空头排列（短期均线在下，看跌信号）"
        else:
            result["arrangement"] = "均线缠绕（方向不明，震荡格局）"
    else:
        result["arrangement"] = "数据不足，无法判断"

    return result


def calc_macd(df: pd.DataFrame, fast=12, slow=26, signal=9) -> dict:
    """计算 MACD"""
    if len(df) < slow + signal:
        return {"DIF": None, "DEA": None, "MACD": None, "signal": "数据不足"}

    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_hist = (dif - dea) * 2

    df["DIF"] = dif
    df["DEA"] = dea
    df["MACD_hist"] = macd_hist

    current_dif = round(dif.iloc[-1], 4)
    current_dea = round(dea.iloc[-1], 4)
    current_macd = round(macd_hist.iloc[-1], 4)

    # 金叉/死叉判断
    if len(df) >= 2:
        prev_dif = dif.iloc[-2]
        prev_dea = dea.iloc[-2]
        if prev_dif <= prev_dea and current_dif > current_dea:
            signal_str = "金叉（看涨信号）"
        elif prev_dif >= prev_dea and current_dif < current_dea:
            signal_str = "死叉（看跌信号）"
        elif current_dif > current_dea:
            signal_str = "多头运行（DIF 在 DEA 之上）"
        else:
            signal_str = "空头运行（DIF 在 DEA 之下）"
    else:
        signal_str = "数据不足"

    return {
        "DIF": current_dif,
        "DEA": current_dea,
        "MACD": current_macd,
        "signal": signal_str,
    }


def calc_rsi(df: pd.DataFrame, period=14) -> dict:
    """计算 RSI"""
    if len(df) < period + 1:
        return {"RSI": None, "status": "数据不足"}

    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    current_rsi = round(rsi.iloc[-1], 2) if not pd.isna(rsi.iloc[-1]) else None

    if current_rsi is None:
        status = "数据不足"
    elif current_rsi > 80:
        status = "严重超买（>80，强烈卖出信号）"
    elif current_rsi > 70:
        status = "超买（>70，注意风险）"
    elif current_rsi < 20:
        status = "严重超卖（<20，强烈买入信号）"
    elif current_rsi < 30:
        status = "超卖（<30，可能反弹）"
    else:
        status = "中性"

    return {"RSI": current_rsi, "status": status}


def calc_support_resistance(df: pd.DataFrame) -> dict:
    """计算支撑位和压力位"""
    if len(df) < 5:
        return {"support": [], "resistance": [], "note": "数据不足"}

    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    current = closes[-1]

    # 方法：找近期高低点
    support_levels = []
    resistance_levels = []

    # 近期低点作为支撑
    for i in range(2, len(lows) - 2):
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i - 2] and lows[i] <= lows[i + 1] and lows[i] <= lows[i + 2]:
            if lows[i] < current:
                support_levels.append(round(lows[i], 4))

    # 近期高点作为压力
    for i in range(2, len(highs) - 2):
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i - 2] and highs[i] >= highs[i + 1] and highs[i] >= highs[i + 2]:
            if highs[i] > current:
                resistance_levels.append(round(highs[i], 4))

    # 去重并排序
    support_levels = sorted(set(support_levels), reverse=True)[:3]  # 最近的3个支撑
    resistance_levels = sorted(set(resistance_levels))[:3]  # 最近的3个压力

    # 补充：用均线作为动态支撑/压力
    for period in [20, 60]:
        if len(df) >= period:
            ma = df["close"].rolling(window=period).mean().iloc[-1]
            if ma < current and round(ma, 4) not in support_levels:
                support_levels.append(round(ma, 4))
            elif ma > current and round(ma, 4) not in resistance_levels:
                resistance_levels.append(round(ma, 4))

    return {
        "support": sorted(support_levels, reverse=True)[:3],
        "resistance": sorted(resistance_levels)[:3],
    }


def calc_gaps(df: pd.DataFrame) -> list:
    """识别缺口"""
    gaps = []
    if len(df) < 2:
        return gaps

    for i in range(1, len(df)):
        prev_high = df["high"].iloc[i - 1]
        prev_low = df["low"].iloc[i - 1]
        curr_open = df["open"].iloc[i]
        curr_low = df["low"].iloc[i]
        curr_high = df["high"].iloc[i]

        # 向上缺口：今日最低 > 昨日最高
        if curr_low > prev_high:
            gap_size = curr_low - prev_high
            gap_pct = round(gap_size / prev_high * 100, 2)
            # 检查后续是否回补
            filled = False
            for j in range(i + 1, len(df)):
                if df["low"].iloc[j] <= prev_high:
                    filled = True
                    break
            gaps.append({
                "type": "向上缺口",
                "date": str(df["date"].iloc[i].date()) if hasattr(df["date"].iloc[i], "date") else str(df["date"].iloc[i]),
                "gap_top": round(curr_low, 4),
                "gap_bottom": round(prev_high, 4),
                "size": round(gap_size, 4),
                "size_pct": gap_pct,
                "filled": filled,
                "role": "支撑" if not filled else "已回补",
            })

        # 向下缺口：今日最高 < 昨日最低
        elif curr_high < prev_low:
            gap_size = prev_low - curr_high
            gap_pct = round(gap_size / prev_low * 100, 2)
            filled = False
            for j in range(i + 1, len(df)):
                if df["high"].iloc[j] >= prev_low:
                    filled = True
                    break
            gaps.append({
                "type": "向下缺口",
                "date": str(df["date"].iloc[i].date()) if hasattr(df["date"].iloc[i], "date") else str(df["date"].iloc[i]),
                "gap_top": round(prev_low, 4),
                "gap_bottom": round(curr_high, 4),
                "size": round(gap_size, 4),
                "size_pct": gap_pct,
                "filled": filled,
                "role": "压力" if not filled else "已回补",
            })

    return gaps


def calc_volume_analysis(df: pd.DataFrame) -> dict:
    """成交量分析"""
    if len(df) < 5 or "volume" not in df.columns:
        return {"trend": "数据不足"}

    volumes = df["volume"].values
    recent_5 = volumes[-5:]
    avg_20 = np.mean(volumes[-20:]) if len(volumes) >= 20 else np.mean(volumes)

    current_vol = volumes[-1]
    prev_vol = volumes[-2] if len(volumes) > 1 else current_vol

    # 量比
    vol_ratio = round(current_vol / avg_20, 2) if avg_20 > 0 else 0

    # 判断
    if vol_ratio > 2:
        trend = "显著放量"
    elif vol_ratio > 1.3:
        trend = "温和放量"
    elif vol_ratio < 0.5:
        trend = "显著缩量"
    elif vol_ratio < 0.8:
        trend = "温和缩量"
    else:
        trend = "量能平稳"

    # 量价配合
    price_up = df["close"].iloc[-1] > df["close"].iloc[-2] if len(df) > 1 else False
    if price_up and vol_ratio > 1.3:
        vol_price = "量价齐升（健康上涨）"
    elif price_up and vol_ratio < 0.8:
        vol_price = "缩量上涨（上涨持续性存疑）"
    elif not price_up and vol_ratio > 1.3:
        vol_price = "放量下跌（恐慌抛售信号）"
    elif not price_up and vol_ratio < 0.8:
        vol_price = "缩量下跌（抛压减弱，可能企稳）"
    else:
        vol_price = "量价关系正常"

    return {
        "current_volume": int(current_vol),
        "avg_20_volume": int(avg_20),
        "vol_ratio": vol_ratio,
        "trend": trend,
        "vol_price_relation": vol_price,
    }


def judge_trend(df: pd.DataFrame, ma_result: dict, macd_result: dict, rsi_result: dict) -> dict:
    """综合趋势判断"""
    signals = {"bullish": 0, "bearish": 0, "neutral": 0}

    # 均线信号
    arr = ma_result.get("arrangement", "")
    if "多头" in arr:
        signals["bullish"] += 2
    elif "空头" in arr:
        signals["bearish"] += 2
    else:
        signals["neutral"] += 1

    # MACD 信号
    macd_signal = macd_result.get("signal", "")
    if "金叉" in macd_signal or "多头" in macd_signal:
        signals["bullish"] += 1.5
    elif "死叉" in macd_signal or "空头" in macd_signal:
        signals["bearish"] += 1.5

    # RSI 信号
    rsi_val = rsi_result.get("RSI")
    if rsi_val is not None:
        if rsi_val > 70:
            signals["bearish"] += 1  # 超买 → 可能回调
        elif rsi_val < 30:
            signals["bullish"] += 1  # 超卖 → 可能反弹
        else:
            signals["neutral"] += 0.5

    # 近期涨跌
    if len(df) >= 5:
        recent_change = (df["close"].iloc[-1] - df["close"].iloc[-5]) / df["close"].iloc[-5] * 100
        if recent_change > 5:
            signals["bullish"] += 1
        elif recent_change < -5:
            signals["bearish"] += 1

    total = signals["bullish"] + signals["bearish"] + signals["neutral"]
    if total == 0:
        total = 1

    bull_pct = round(signals["bullish"] / total * 100)
    bear_pct = round(signals["bearish"] / total * 100)
    neutral_pct = 100 - bull_pct - bear_pct

    if signals["bullish"] > signals["bearish"] * 1.5:
        trend = "上升趋势"
        strength = "强" if signals["bullish"] > 4 else "中"
    elif signals["bearish"] > signals["bullish"] * 1.5:
        trend = "下降趋势"
        strength = "强" if signals["bearish"] > 4 else "中"
    else:
        trend = "震荡整理"
        strength = ""

    return {
        "trend": trend,
        "strength": strength,
        "up_probability": bull_pct,
        "down_probability": bear_pct,
        "sideways_probability": neutral_pct,
        "signal_scores": {
            "bullish": round(signals["bullish"], 1),
            "bearish": round(signals["bearish"], 1),
            "neutral": round(signals["neutral"], 1),
        },
    }


# ============================================================
#  主分析流程
# ============================================================

def analyze(data: dict) -> dict:
    """执行完整技术分析"""
    history = data.get("history", [])
    if not history:
        return {"error": "无历史数据可分析"}

    df = build_dataframe(history)

    # 计算各项指标
    ma = calc_ma(df)
    macd = calc_macd(df)
    rsi = calc_rsi(df)
    support_resistance = calc_support_resistance(df)
    gaps = calc_gaps(df)
    volume = calc_volume_analysis(df)
    trend = judge_trend(df, ma, macd, rsi)

    result = {
        "code": data.get("code", ""),
        "asset_type": data.get("asset_type", ""),
        "realtime": data.get("realtime", {}),
        "source": data.get("source", ""),
        "data_points": len(df),
        "analysis": {
            "moving_averages": ma,
            "macd": macd,
            "rsi": rsi,
            "support_resistance": support_resistance,
            "gaps": gaps,
            "gap_count": len(gaps),
            "unfilled_gaps": [g for g in gaps if not g["filled"]],
            "volume": volume,
            "trend": trend,
        },
    }

    return result


def main():
    parser = argparse.ArgumentParser(description="技术分析")
    parser.add_argument("--data_file", required=True, help="market data JSON 文件路径")
    parser.add_argument("--output", default=None, help="分析结果输出路径")
    args = parser.parse_args()

    data = load_data(args.data_file)
    result = analyze(data)

    output_json = json.dumps(result, ensure_ascii=False, indent=2, default=str)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_json)
        print(f"分析结果已保存到: {args.output}", file=sys.stderr)
    else:
        code = data.get("code", "unknown").replace(".", "_")
        filename = f"analysis_{code}.json"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(output_json)
        print(f"分析结果已保存到: {filename}", file=sys.stderr)

    print(output_json)


if __name__ == "__main__":
    main()
