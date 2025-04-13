import requests
from mcp.server.fastmcp import FastMCP
import logging
import os

# 配置日志
logging.basicConfig(level=logging.ERROR)

# 从环境变量获取和风天气 API Key 和 URL 前缀，具体请在和风天气控制台 https://console.qweather.com/home?lang=zh 申请
HEWEATHER_API_KEY = os.environ.get("HEWEATHER_API_KEY", "你的API密钥")
WEATHER_URL_PREFIX = os.environ.get("WEATHER_URL_PREFIX", "你的API请求前缀")

# 初始化 MCP 服务器
mcp = FastMCP("WeatherServer", log_level="ERROR")  # 日志级别可以调整为 DEBUG、INFO、WARNING、ERROR、CRITICAL

def get_city_id(city_name: str) -> str:
    """根据中文城市名获取和风天气 location ID"""
    url = f"{WEATHER_URL_PREFIX}/geo/v2/city/lookup"
    params = {
        "location": city_name
    }
    # 添加请求头
    headers = {
        "X-QW-Api-Key": HEWEATHER_API_KEY
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        # 检查响应状态码
        response.raise_for_status()  
        logging.info(f"get_city_id 响应内容: {response.text}")
        data = response.json()
        if data.get("code") == "200" and data.get("location"):
            return data["location"][0]["id"]
        else:
            raise ValueError(f"找不到城市: {city_name}，错误信息: {data}")
    except requests.RequestException as e:
        logging.error(f"get_city_id 请求出错: {e}")
        raise
    except ValueError as e:
        logging.error(f"get_city_id 解析 JSON 出错: {e}")
        raise

def get_weather(city_name: str) -> str:
    """根据城市中文名返回当前天气中文描述"""
    try:
        location_id = get_city_id(city_name)
        url = f"{WEATHER_URL_PREFIX}/v7/weather/now"
        params = {
            "location": location_id
        }
        # 添加请求头
        headers = {
            "X-QW-Api-Key": HEWEATHER_API_KEY
        }
        response = requests.get(url, params=params, headers=headers)
        # 检查响应状态码
        response.raise_for_status()  
        logging.info(f"get_weather 响应内容: {response.text}")
        data = response.json()
        if data.get("code") != "200":
            return f"天气查询失败：{data.get('code')}"
        now = data["now"]
        return (
            f"🌍 城市: {city_name}\n"
            f"🌤 天气: {now['text']}\n"
            f"🌡 温度: {now['temp']}°C\n"
            f"💧 湿度: {now['humidity']}%\n"
            f"🌬 风速: {now['windSpeed']} m/s\n"
        )
    except requests.RequestException as e:
        return f"查询出错：网络请求失败，错误信息: {str(e)}"
    except ValueError as e:
        return f"查询出错：解析响应数据失败，响应内容: {response.text}, 错误信息: {str(e)}"

@mcp.tool('query_weather', '查询城市天气')
def query_weather(city: str) -> str:
    """
        输入指定城市的中文名称，返回当前天气查询结果。
        :param city: 城市名称
        :return: 格式化后的天气信息
        """
    return get_weather(city)


if __name__ == "__main__":
    # 以标准 I/O 方式运行 MCP 服务器
    mcp.run(transport='stdio')