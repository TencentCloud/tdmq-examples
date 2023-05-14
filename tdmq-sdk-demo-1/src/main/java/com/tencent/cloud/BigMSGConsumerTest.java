package com.tencent.cloud;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.com.google.common.base.Strings;

public class BigMSGConsumerTest {

    public static void main(String[] args) throws Exception {

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.INFO);
        ctx.updateLoggers();

        System.out.printf(" args: %s \n",args);
        //接入地址
        String SERVICE_URL = args[0];
        //token
        String PROD_TOKEN = args[1];
        //topic名称
        String TOPIC_NAME = args[2];
        //订阅名称
        String SUB_NAME = args[3];
        //消息条数
        int msgCount = Integer.valueOf(args[4]);
        //间隔
        int sleepms = Integer.valueOf(args[5]);
        //tag
        String tag = args[6];

        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)//完整复制控制台接入点地址
                .authentication(AuthenticationFactory.token(PROD_TOKEN))
                .statsInterval(5, TimeUnit.SECONDS)
                .build();

        System.out.println("=================================Start consume msg=====================================");
        // 订阅相关参数，可用来设置订阅标签(TAG)
        HashMap<String, String> subProperties = new HashMap<>();
        if (!Strings.isNullOrEmpty(tag) && !tag.equals("null")){
            subProperties.put(tag,"1");
        }
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC_NAME)
                .subscriptionName(SUB_NAME)
                .subscriptionType(SubscriptionType.Shared)
                .autoUpdatePartitionsInterval(1000,TimeUnit.MILLISECONDS)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .ackTimeout(1000,TimeUnit.MILLISECONDS)
                .subscriptionProperties(subProperties)
                .subscribe();

        for(int j=0; j< msgCount; j++){
            TimeUnit.MILLISECONDS.sleep(sleepms);
            Message msg = consumer.receive();
            System.out.println("消息大小为:"+msg.getData().length/1024/1024+"MB");
            System.out.printf("%s 消费完成 第%d条 msgid:%s tag:%s key:%s sequenceID:%d\n ", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")),j+1,msg.getMessageId(),msg.getProperties(),msg.getKey(),msg.getSequenceId());
            consumer.negativeAcknowledge(msg);
            consumer.acknowledge(msg);
            System.out.printf("%s ask完成 第%d条 msgid:%s \n ",LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")),j+1,msg.getMessageId());
        }
    }
}
