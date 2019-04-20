# bohr
实时三重量化交易系统

创建python虚拟环境
virtualenv -p /usr/bin/python3 env
source ./env/bin/activate

talib库安装
alib库有超多现成的方法，不用辛辛苦苦造轮子。上MACD、动量、rsi、移动均线等等

相应操作系统的安装包在这里https://www.lfd.uci.edu/~gohlke/pythonlibs/
windows：
pip install .\TA_Lib-0.4.17-cp36-cp36m-win_amd64.whl
ubuntu：
wget https://sourceforge.net/projects/ta-lib/files/ta-lib/0.4.0/ta-lib-0.4.0-src.tar.gz

$ tar -xvf ta-lib-0.4.0-src.tar.gz  # 解压
$ cd ta-lib # 进入目录
$ ./configure --prefix=/usr
$ make
$ make install
将编译好的文件复制到虚拟目录中
$ cp /usr/lib/libta_lib*  ~/bohr/env/lib
最后：
pip install ta-lib


#etcd
wget https://github.com/etcd-io/etcd/releases/download/v3.3.12/etcd-v3.3.12-linux-amd64.tar.gz

docker
sudo docker rm bohr-etcd
sudo docker run \
  -d \
  -p 2379:2379 \
  -p 2380:2380 \
  -p 4001:4001 \
  -p 7001:7001 \
  --name bohr-etcd \
  elcolio/etcd:latest