FROM registry.cn-hangzhou.aliyuncs.com/chenxl/python3.6
RUN apt-get update
WORKDIR /usr/src/app
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN pip install --upgrade pip
COPY ./ .
RUN pip install -r requirements.txt

CMD ["run.sh",]