# store
rocketmq的磁盘存储模块，我增加了延时消息

# 架构
为rocketmq的store模块增加了延时消息，精确到秒，开源自带的延时消息只能支持18个level，改进的延时消息精确到秒没有最大限制。

文档
https://www.cnblogs.com/hzmark/p/mq-delay-msg.html

# 使用
rocketmq的接口不变，只是timelevel的意义变成了秒为单位的延时值。
生产者：
        public MessageExtBrokerInner buildScheduleMessage(long index) {
    		Random rr=new Random();
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setKeys("Hello"+index);
        msg.setBody(MessageBody);
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        msg.setDelayTimeLevel(rr.nextInt(2500)+20);
        return msg;
    }
    
 消费者：
 消费者照常消费，延时时间到了消息就能消费到。
