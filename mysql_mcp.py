from mcp.server.fastmcp import FastMCP
import pymysql

mcp = FastMCP("mysql_mcp", log_level="ERROR")

@mcp.tool()
def analysis_data(amt: int) -> int:
    conn = pymysql.connect(
        host="192.168.147.130",
        port=3306,
        user="django",
        password="gw123456",
        database="yifei_dev"
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `tmall_天猫_推广发票_总览` where `开票金额（元）` > {amt}")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result

if __name__ == "__main__":
    try:
        mcp.run(transport='stdio')
    except Exception as e:
        logger.error(f"服务器运行失败: {str(e)}")
        sys.exit(1)