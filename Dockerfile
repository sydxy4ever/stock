# 使用轻量级 Python 3.11 镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN apt-get update && apt-get install -y tzdata && rm -rf /var/lib/apt/lists/*

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 将项目文件复制到容器中（包含所有子目录：fetchers/ tools/ strategy/ analyze/）
COPY . .

# 创建输出目录
RUN mkdir -p /app/output


# 设置环境变量，DB_PATH 需指向 /data 中
ENV DB_PATH=/data/stock_data.db
ENV TURNOVER_DB_PATH=/data/turnover_surge.db

# 默认运行调度器
CMD ["python", "-u", "scheduler.py"]
