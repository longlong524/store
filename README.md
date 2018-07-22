# store
rocketmq的磁盘存储模块，我增加了延时消息

# 架构
为rocketmq的store模块增加了延时消息，精确到秒，开源自带的延时消息只能支持1到18秒18个level，改进的延时消息精确到秒最大为7200秒。

文档
https://www.cnblogs.com/hzmark/p/mq-delay-msg.html

