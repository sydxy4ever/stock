# 使用轻量级 Python 镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置系统时区（这对金融脚本很重要，确保时间与北京时间同步）
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' > /etc/timezone

# 复制依赖文件并安装
# 使用清华大学镜像源以加快国内下载速度
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制脚本到容器
COPY stock-screener.py .

# 运行脚本
CMD ["python", "stock-screener.py"]