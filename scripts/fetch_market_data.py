#!/usr/bin/env python3
"""
投资分析 Skill — 市场数据获取模块
支持：A股/港股/美股（多数据源自动切换）+ 加密货币（CoinGecko/Binance）
所有源码透明可审计，无编译二进制。
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    print("错误：需要 requests 库。请运行: pip install requests")
    sys.exit(1)


# ============================================================
#  股票数据获取
# ============================================================

def fetch_stock_sina(code: str, days: int) -> dict:
    """新浪财经 — A股/港股实时+历史数据"""
    result = {"source": "sina", "success": False}

    # 格式化代码
    sina_code = _format_sina_code(code)
    if not sina_code:
        result["error"] = f"无法识别股票代码格式: {code}"
        return result

    try:
        # 实时行情
        url = f"https://hq.sinajs.cn/list={sina_code}"
        headers = {"Referer": "https://finance.sina.com.cn"}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "gbk"
        text = resp.text.strip()

        if "FAILED" in text or '=""' in text or len(text) < 20:
            result["error"] = "新浪财经返回空数据"
            return result

        data_str = text.split('"')[1]
        fields = data_str.split(",")

        if sina_code.startswith(("sh", "sz")):
            # A股格式：名称,今开,昨收,当前价,最高,最低,...,成交量,成交额,...,日期,时间
            if len(fields) < 32:
                result["error"] = "新浪A股数据字段不足"
                return result
            realtime = {
                "name": fields[0],
                "open": float(fields[1]) if fields[1] else 0,
                "prev_close": float(fields[2]) if fields[2] else 0,
                "price": float(fields[3]) if fields[3] else 0,
                "high": float(fields[4]) if fields[4] else 0,
                "low": float(fields[5]) if fields[5] else 0,
                "volume": int(float(fields[8])) if fields[8] else 0,
                "amount": float(fields[9]) if fields[9] else 0,
                "date": fields[30] if len(fields) > 30 else "",
                "time": fields[31] if len(fields) > 31 else "",
            }
        elif sina_code.startswith("hk"):
            # 港股格式
            if len(fields) < 10:
                result["error"] = "新浪港股数据字段不足"
                return result
            realtime = {
                "name": fields[1] if len(fields) > 1 else code,
                "open": float(fields[2]) if fields[2] else 0,
                "prev_close": float(fields[3]) if fields[3] else 0,
                "price": float(fields[6]) if fields[6] else 0,
                "high": float(fields[4]) if fields[4] else 0,
                "low": float(fields[5]) if fields[5] else 0,
                "volume": int(float(fields[12])) if len(fields) > 12 and fields[12] else 0,
                "amount": float(fields[11]) if len(fields) > 11 and fields[11] else 0,
                "date": fields[17] if len(fields) > 17 else "",
                "time": fields[18] if len(fields) > 18 else "",
            }
        else:
            result["error"] = f"不支持的新浪代码格式: {sina_code}"
            return result

        if realtime["prev_close"] > 0:
            realtime["change_pct"] = round(
                (realtime["price"] - realtime["prev_close"]) / realtime["prev_close"] * 100, 2
            )
        else:
            realtime["change_pct"] = 0

        # 历史K线（新浪）
        history = _fetch_sina_history(sina_code, days)

        result.update({
            "success": True,
            "asset_type": "stock",
            "code": code,
            "realtime": realtime,
            "history": history,
        })

    except requests.RequestException as e:
        result["error"] = f"新浪财经请求失败: {str(e)}"
    except (IndexError, ValueError) as e:
        result["error"] = f"新浪财经数据解析失败: {str(e)}"

    return result


def _format_sina_code(code: str) -> str:
    """将用户输入的代码格式化为新浪格式"""
    code = code.strip().upper()

    # 港股：00700.HK → hk00700
    if code.endswith(".HK"):
        return "hk" + code.replace(".HK", "")

    # A股带后缀：000001.SZ → sz000001
    if code.endswith(".SZ"):
        return "sz" + code.replace(".SZ", "")
    if code.endswith(".SH"):
        return "sh" + code.replace(".SH", "")

    # A股带前缀：sh600000
    if code.lower().startswith(("sh", "sz")):
        return code.lower()

    # 纯数字A股：自动判断交易所
    if code.isdigit() and len(code) == 6:
        if code.startswith(("6", "9")):
            return "sh" + code
        else:
            return "sz" + code

    # 美股：暂不支持新浪
    return ""


def _fetch_sina_history(sina_code: str, days: int) -> list:
    """获取新浪历史K线数据"""
    history = []
    try:
        # 使用新浪的历史行情接口
        raw_code = sina_code.replace("sh", "").replace("sz", "").replace("hk", "")
        market = "0" if sina_code.startswith("sh") else "1"

        url = (
            f"https://quotes.sina.cn/cn/api/jsonp.php/var/"
            f"CN_MarketDataService.getKLineData?"
            f"symbol={sina_code}&scale=240&ma=no&datalen={days}"
        )
        resp = requests.get(url, timeout=10)
        resp.encoding = "utf-8"
        text = resp.text

        # 解析 JSONP
        start = text.find("(")
        end = text.rfind(")")
        if start >= 0 and end > start:
            json_str = text[start + 1:end]
            data = json.loads(json_str)
            for item in data:
                history.append({
                    "date": item.get("day", ""),
                    "open": float(item.get("open", 0)),
                    "high": float(item.get("high", 0)),
                    "low": float(item.get("low", 0)),
                    "close": float(item.get("close", 0)),
                    "volume": int(float(item.get("volume", 0))),
                })
    except Exception:
        pass
    return history


def fetch_stock_eastmoney(code: str, days: int) -> dict:
    """东方财富 — A股/港股实时+历史数据"""
    result = {"source": "eastmoney", "success": False}

    secid = _format_eastmoney_code(code)
    if not secid:
        result["error"] = f"无法识别代码: {code}"
        return result

    try:
        # 实时行情
        url = (
            f"https://push2.eastmoney.com/api/qt/stock/get?"
            f"secid={secid}&fields=f43,f44,f45,f46,f47,f48,f50,f51,f52,f55,f57,f58,f60,f170"
            f"&_={int(time.time() * 1000)}"
        )
        resp = requests.get(url, timeout=10)
        data = resp.json()

        if data.get("data") is None:
            result["error"] = "东方财富返回空数据"
            return result

        d = data["data"]
        # 东方财富价格单位是"分"(整数)，需要除以 100（部分接口）
        # 但有时候直接就是元，看字段值大小判断
        price = d.get("f43", 0)
        prev_close = d.get("f60", 0)

        # 如果值大于正常范围，说明是分为单位
        if isinstance(price, int) and price > 10000 and not code.upper().startswith("6"):
            divisor = 100
        elif isinstance(price, int) and price > 100000:
            divisor = 100
        else:
            divisor = 1

        realtime = {
            "name": d.get("f58", code),
            "price": price / divisor if divisor > 1 else price,
            "prev_close": prev_close / divisor if divisor > 1 else prev_close,
            "open": d.get("f46", 0) / divisor if divisor > 1 else d.get("f46", 0),
            "high": d.get("f44", 0) / divisor if divisor > 1 else d.get("f44", 0),
            "low": d.get("f45", 0) / divisor if divisor > 1 else d.get("f45", 0),
            "volume": d.get("f47", 0),
            "amount": d.get("f48", 0),
            "change_pct": d.get("f170", 0) / 100 if isinstance(d.get("f170"), int) else d.get("f170", 0),
        }

        # 历史K线
        history = _fetch_eastmoney_history(secid, days)

        result.update({
            "success": True,
            "asset_type": "stock",
            "code": code,
            "realtime": realtime,
            "history": history,
        })

    except requests.RequestException as e:
        result["error"] = f"东方财富请求失败: {str(e)}"
    except (KeyError, ValueError) as e:
        result["error"] = f"东方财富数据解析失败: {str(e)}"

    return result


def _format_eastmoney_code(code: str) -> str:
    """格式化为东方财富 secid 格式（市场.代码）"""
    code = code.strip().upper()

    if code.endswith(".HK"):
        return "116." + code.replace(".HK", "")
    if code.endswith(".SZ"):
        return "0." + code.replace(".SZ", "")
    if code.endswith(".SH"):
        return "1." + code.replace(".SH", "")

    if code.lower().startswith("sh"):
        return "1." + code[2:]
    if code.lower().startswith("sz"):
        return "0." + code[2:]

    if code.isdigit() and len(code) == 6:
        if code.startswith(("6", "9")):
            return "1." + code
        else:
            return "0." + code

    # 美股
    if code.isalpha():
        return "105." + code

    return ""


def _fetch_eastmoney_history(secid: str, days: int) -> list:
    """获取东方财富历史K线"""
    history = []
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days + 10)).strftime("%Y%m%d")

        url = (
            f"https://push2his.eastmoney.com/api/qt/stock/kline/get?"
            f"secid={secid}&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55,f56,f57"
            f"&klt=101&fqt=1&beg={start_date}&end={end_date}"
            f"&_={int(time.time() * 1000)}"
        )
        resp = requests.get(url, timeout=10)
        data = resp.json()

        klines = data.get("data", {}).get("klines", [])
        for line in klines[-days:]:
            parts = line.split(",")
            if len(parts) >= 7:
                history.append({
                    "date": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": int(float(parts[5])),
                })
    except Exception:
        pass
    return history


def fetch_stock_xueqiu(code: str, days: int) -> dict:
    """雪球 — 备用数据源"""
    result = {"source": "xueqiu", "success": False}

    xq_code = _format_xueqiu_code(code)
    if not xq_code:
        result["error"] = f"无法识别代码: {code}"
        return result

    try:
        session = requests.Session()
        # 先获取 cookie
        session.get("https://xueqiu.com/", timeout=10)

        # 实时行情
        url = f"https://stock.xueqiu.com/v5/stock/quote.json?symbol={xq_code}"
        resp = session.get(url, timeout=10)
        data = resp.json()

        if data.get("data") is None or data["data"].get("quote") is None:
            result["error"] = "雪球返回空数据"
            return result

        q = data["data"]["quote"]
        realtime = {
            "name": q.get("name", code),
            "price": q.get("current", 0),
            "prev_close": q.get("last_close", 0),
            "open": q.get("open", 0),
            "high": q.get("high", 0),
            "low": q.get("low", 0),
            "volume": q.get("volume", 0),
            "amount": q.get("amount", 0),
            "change_pct": q.get("percent", 0),
        }

        # 历史K线
        end_ts = int(time.time() * 1000)
        begin_ts = int((time.time() - days * 86400) * 1000)
        kline_url = (
            f"https://stock.xueqiu.com/v5/stock/chart/kline.json?"
            f"symbol={xq_code}&begin={end_ts}&period=day&type=before"
            f"&count=-{days}"
        )
        kresp = session.get(kline_url, timeout=10)
        kdata = kresp.json()

        history = []
        items = kdata.get("data", {}).get("item", [])
        columns = kdata.get("data", {}).get("column", [])
        if items and columns:
            for item in items:
                row = dict(zip(columns, item))
                ts = row.get("timestamp", 0)
                date_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d") if ts else ""
                history.append({
                    "date": date_str,
                    "open": row.get("open", 0),
                    "high": row.get("high", 0),
                    "low": row.get("low", 0),
                    "close": row.get("close", 0),
                    "volume": row.get("volume", 0),
                })

        result.update({
            "success": True,
            "asset_type": "stock",
            "code": code,
            "realtime": realtime,
            "history": history,
        })

    except requests.RequestException as e:
        result["error"] = f"雪球请求失败: {str(e)}"
    except (KeyError, ValueError) as e:
        result["error"] = f"雪球数据解析失败: {str(e)}"

    return result


def _format_xueqiu_code(code: str) -> str:
    """格式化为雪球代码格式"""
    code = code.strip().upper()

    if code.endswith(".HK"):
        return code.replace(".HK", "")  # 雪球港股直接用数字
    if code.endswith(".SZ"):
        return "SZ" + code.replace(".SZ", "")
    if code.endswith(".SH"):
        return "SH" + code.replace(".SH", "")

    if code.lower().startswith("sh"):
        return "SH" + code[2:]
    if code.lower().startswith("sz"):
        return "SZ" + code[2:]

    if code.isdigit() and len(code) == 6:
        if code.startswith(("6", "9")):
            return "SH" + code
        else:
            return "SZ" + code

    # 美股
    if code.isalpha():
        return code

    return ""


# ============================================================
#  加密货币数据获取
# ============================================================

def fetch_crypto_coingecko(code: str, days: int) -> dict:
    """CoinGecko — 免费加密货币行情"""
    result = {"source": "coingecko", "success": False}

    coin_id = _crypto_code_to_coingecko_id(code)
    if not coin_id:
        result["error"] = f"未识别的加密货币代码: {code}"
        return result

    try:
        # 当前价格
        url = (
            f"https://api.coingecko.com/api/v3/coins/{coin_id}?"
            f"localization=false&tickers=false&community_data=false"
            f"&developer_data=false"
        )
        resp = requests.get(url, timeout=15)
        if resp.status_code == 429:
            result["error"] = "CoinGecko API 限速，请稍后重试"
            return result
        data = resp.json()

        market = data.get("market_data", {})
        realtime = {
            "name": data.get("name", code),
            "symbol": data.get("symbol", code).upper(),
            "price": market.get("current_price", {}).get("usd", 0),
            "price_cny": market.get("current_price", {}).get("cny", 0),
            "change_pct_24h": market.get("price_change_percentage_24h", 0),
            "change_pct_7d": market.get("price_change_percentage_7d", 0),
            "change_pct_30d": market.get("price_change_percentage_30d", 0),
            "high_24h": market.get("high_24h", {}).get("usd", 0),
            "low_24h": market.get("low_24h", {}).get("usd", 0),
            "market_cap": market.get("market_cap", {}).get("usd", 0),
            "total_volume": market.get("total_volume", {}).get("usd", 0),
            "ath": market.get("ath", {}).get("usd", 0),
            "ath_change_pct": market.get("ath_change_percentage", {}).get("usd", 0),
            "atl": market.get("atl", {}).get("usd", 0),
        }

        # 历史数据
        time.sleep(1)  # CoinGecko 限速
        hist_url = (
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?"
            f"vs_currency=usd&days={days}&interval=daily"
        )
        hist_resp = requests.get(hist_url, timeout=15)
        hist_data = hist_resp.json()

        history = []
        prices = hist_data.get("prices", [])
        volumes = hist_data.get("total_volumes", [])

        for i, (ts, price) in enumerate(prices):
            date_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            vol = volumes[i][1] if i < len(volumes) else 0
            history.append({
                "date": date_str,
                "close": price,
                "volume": vol,
            })

        # 补充 OHLC 数据
        time.sleep(1)
        ohlc_days = min(days, 90)  # CoinGecko OHLC 最多90天
        ohlc_url = (
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?"
            f"vs_currency=usd&days={ohlc_days}"
        )
        ohlc_resp = requests.get(ohlc_url, timeout=15)
        ohlc_data = ohlc_resp.json()

        if isinstance(ohlc_data, list):
            ohlc_by_date = {}
            for item in ohlc_data:
                if len(item) >= 5:
                    d = datetime.fromtimestamp(item[0] / 1000).strftime("%Y-%m-%d")
                    if d not in ohlc_by_date:
                        ohlc_by_date[d] = {
                            "open": item[1],
                            "high": item[2],
                            "low": item[3],
                            "close": item[4],
                        }
                    else:
                        ohlc_by_date[d]["high"] = max(ohlc_by_date[d]["high"], item[2])
                        ohlc_by_date[d]["low"] = min(ohlc_by_date[d]["low"], item[3])
                        ohlc_by_date[d]["close"] = item[4]

            for h in history:
                if h["date"] in ohlc_by_date:
                    h.update(ohlc_by_date[h["date"]])

        result.update({
            "success": True,
            "asset_type": "crypto",
            "code": code.upper(),
            "realtime": realtime,
            "history": history,
        })

    except requests.RequestException as e:
        result["error"] = f"CoinGecko 请求失败: {str(e)}"
    except (KeyError, ValueError) as e:
        result["error"] = f"CoinGecko 数据解析失败: {str(e)}"

    return result


def fetch_crypto_binance(code: str, days: int) -> dict:
    """Binance — 备用加密货币数据源"""
    result = {"source": "binance", "success": False}

    symbol = code.strip().upper()
    if not symbol.endswith("USDT"):
        symbol = symbol + "USDT"

    try:
        # K线数据
        end_time = int(time.time() * 1000)
        start_time = end_time - days * 86400 * 1000

        url = (
            f"https://api.binance.com/api/v3/klines?"
            f"symbol={symbol}&interval=1d&startTime={start_time}&endTime={end_time}"
            f"&limit={days}"
        )
        resp = requests.get(url, timeout=15)

        if resp.status_code != 200:
            result["error"] = f"Binance 返回状态码: {resp.status_code}"
            return result

        klines = resp.json()
        if not klines:
            result["error"] = "Binance 返回空数据"
            return result

        history = []
        for k in klines:
            history.append({
                "date": datetime.fromtimestamp(k[0] / 1000).strftime("%Y-%m-%d"),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            })

        # 最新一条作为实时数据
        latest = klines[-1]
        price = float(latest[4])
        prev_close = float(klines[-2][4]) if len(klines) > 1 else price
        change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close else 0

        realtime = {
            "name": code.upper(),
            "symbol": code.upper(),
            "price": price,
            "open": float(latest[1]),
            "high": float(latest[2]),
            "low": float(latest[3]),
            "volume": float(latest[5]),
            "change_pct_24h": change_pct,
        }

        # 24h ticker 补充
        ticker_url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        ticker_resp = requests.get(ticker_url, timeout=10)
        if ticker_resp.status_code == 200:
            ticker = ticker_resp.json()
            realtime.update({
                "price": float(ticker.get("lastPrice", price)),
                "high_24h": float(ticker.get("highPrice", 0)),
                "low_24h": float(ticker.get("lowPrice", 0)),
                "volume": float(ticker.get("volume", 0)),
                "change_pct_24h": float(ticker.get("priceChangePercent", 0)),
            })

        result.update({
            "success": True,
            "asset_type": "crypto",
            "code": code.upper(),
            "realtime": realtime,
            "history": history,
        })

    except requests.RequestException as e:
        result["error"] = f"Binance 请求失败: {str(e)}"
    except (KeyError, ValueError, IndexError) as e:
        result["error"] = f"Binance 数据解析失败: {str(e)}"

    return result


def _crypto_code_to_coingecko_id(code: str) -> str:
    """常见加密货币代码映射到 CoinGecko ID"""
    mapping = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "BNB": "binancecoin",
        "XRP": "ripple",
        "ADA": "cardano",
        "DOGE": "dogecoin",
        "AVAX": "avalanche-2",
        "DOT": "polkadot",
        "MATIC": "matic-network",
        "LINK": "chainlink",
        "UNI": "uniswap",
        "ATOM": "cosmos",
        "LTC": "litecoin",
        "FIL": "filecoin",
        "APT": "aptos",
        "ARB": "arbitrum",
        "OP": "optimism",
        "SUI": "sui",
        "NEAR": "near",
        "SHIB": "shiba-inu",
        "PEPE": "pepe",
        "TRX": "tron",
        "TON": "the-open-network",
    }
    return mapping.get(code.strip().upper(), "")


# ============================================================
#  统一入口
# ============================================================

def fetch_data(asset_type: str, code: str, days: int, source: str = None) -> dict:
    """统一数据获取入口，支持自动切换"""

    if asset_type == "crypto":
        sources = [
            ("coingecko", fetch_crypto_coingecko),
            ("binance", fetch_crypto_binance),
        ]
    else:
        sources = [
            ("sina", fetch_stock_sina),
            ("eastmoney", fetch_stock_eastmoney),
            ("xueqiu", fetch_stock_xueqiu),
        ]

    # 如果指定了数据源，优先使用
    if source:
        source_map = {s[0]: s[1] for s in sources}
        if source in source_map:
            sources = [(source, source_map[source])] + [s for s in sources if s[0] != source]

    errors = []
    for name, fetch_fn in sources:
        print(f"[数据源] 尝试 {name}...", file=sys.stderr)
        result = fetch_fn(code, days)
        if result.get("success"):
            print(f"[数据源] {name} 成功 ✓", file=sys.stderr)
            return result
        else:
            err = result.get("error", "未知错误")
            errors.append(f"{name}: {err}")
            print(f"[数据源] {name} 失败 ✗ — {err}", file=sys.stderr)

    return {
        "success": False,
        "error": "所有数据源均失败",
        "details": errors,
        "code": code,
    }


def main():
    parser = argparse.ArgumentParser(description="市场数据获取")
    parser.add_argument("--asset_type", required=True, choices=["stock", "crypto"], help="资产类型")
    parser.add_argument("--code", required=True, help="资产代码")
    parser.add_argument("--days", type=int, default=30, help="历史天数")
    parser.add_argument("--source", default=None, help="指定数据源")
    parser.add_argument("--output", default=None, help="输出文件路径")
    args = parser.parse_args()

    result = fetch_data(args.asset_type, args.code, args.days, args.source)

    # 输出
    output_json = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_json)
        print(f"数据已保存到: {args.output}", file=sys.stderr)
    else:
        # 默认保存到当前目录
        filename = f"market_data_{args.code.replace('.', '_')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(output_json)
        print(f"数据已保存到: {filename}", file=sys.stderr)

    print(output_json)


if __name__ == "__main__":
    main()
