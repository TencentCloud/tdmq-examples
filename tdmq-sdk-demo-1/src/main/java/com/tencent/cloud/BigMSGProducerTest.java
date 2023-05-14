package com.tencent.cloud;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.*;
import java.util.concurrent.TimeUnit;

public class BigMSGProducerTest {

    public static void main(String[] args) throws Exception {

        Map<Integer, CompressionType> zipTypeMap = new HashMap<>();
        zipTypeMap.put(1, CompressionType.LZ4);
        zipTypeMap.put(2, CompressionType.ZLIB);
        zipTypeMap.put(3, CompressionType.ZSTD);
        zipTypeMap.put(4, CompressionType.SNAPPY);

        System.out.printf("args: %s \n", args);
        //接入地址
        String SERVICE_URL = args[0];
        //token
        String PROD_TOKEN = args[1];
        //topic名称
        String TOPIC_NAME = args[2];
        //订阅名称
//        String SUB_NAME = args[3];
        //生产条数
        int msgCount = Integer.valueOf(args[4]);
        //生产间隔
        int sleepms = Integer.valueOf(args[5]);
        // 是否开始batch(是否关闭chunk)
        int chunk = Integer.valueOf(args[6]);
        // 是否压缩
        int compre = Integer.valueOf(args[7]);
        // 消息大小 单位为k
        int msgSize = Integer.valueOf(args[8]);
        //压缩方式
        int zipType = Integer.valueOf(args[9]);
        //消息内容
        int messageContentType = Integer.valueOf(args[10]);

        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)//完整复制控制台接入点地址
                .authentication(AuthenticationFactory.token(PROD_TOKEN)).statsInterval(5, TimeUnit.SECONDS)
                .build();
        ProducerBuilder pbu = pulsarClient.newProducer();
        if (chunk > 0) {
            pbu.enableBatching(false);
            pbu.enableChunking(true);
        } else {
            pbu.enableBatching(true);
            pbu.enableChunking(false);
        }
        if (compre > 0) {
            pbu.compressionType(zipTypeMap.get(zipType));
        } else {
            pbu.compressionType(CompressionType.NONE);
        }
        Producer<byte[]> producer = pbu.topic(TOPIC_NAME).create();
        String raw = "ST6H6sW5zYBRalFbVQF0QicoSdNctAQ1zjYgqQfmzaRwYluPLf5r4k8pxZbA5lDOPn3NdnYnvc5dE2dtoO7F51bFrjJgbHsIQfVERgKIddNQq7Dw5RAAS6UvtDB2JVtJcDDwEzrLTRnra16pEpYYlcNMuaezhvtBg2tngGI8Zu8BXxYpDZxHYmainDHCz2jNnzYGa2GPBbvtUEITvdBkmM9Ct9V4xge2uYL4btd1SVwlJ8bduJmV1r22njv3QmjaxCXhT2gwK5lGMq7Xf00426dIMSiNFIltJlINl2ezPVKIRYkGDD63A45phwDOIuKdO4JoFMzd5ps9YDVbXBoNKVG5RhK8O7oPiAFaESL8bkSCU0OIUN8bXqMZXXoMA7fUKlNPQpb3cwOKv9H159vRGr91vxSk7YZF4brh2O7bHnp5IXJzgOUMaBhwHrkhYSlVoo5bHTQx9K10kLAJALiLMP75XDKFzCrwLOB04UCsr3DcjUiY8ASrCEiDaUkOSnuJ6MTO1XKNZaap9zYuxCDioqemMHYzomGOI2cy3DRu34bI3SeyT30tlEJ1klGeujtCu7V1v4WlT6FnzGZi8na3TfGR4RQVfOoBo01V8IndTYFkxcXWbtZe8aw1EJVK2emOs97jQaMMJnoegSUrvx8AVXNrQvnipOgzxANewh4vuR24qgHeL9UAvITaTEDBhKoeoKsheQh5qR6v24PnvA1W4zG2hmLPmCdSCejOl3q3RrqVQ13hwAxqT1JEX9OASHal7GslaK6ajPMqNthYDxIJ1wn5oeVHwHSQThXWWnhoJfZwFCv2OneLbGlamXZAI8KmW2f2qAIfynKB3Tc6K29AO6iqadf1pJR1axb3fajKoUBfu9ZOK0DswaU8Aag5GgI7O33taei2Vdf8peCcmnORmprDrV7TvKg2Q0KNTleYnNxN0DF84eSS4yLBVCYUp1t6p0BQz2U1EYdPjVniBZf5942sZvhAUiLwJgnH8diimkzAnV72QMF7ChIelJ7FEwcf";
        String message = null;
        StringBuilder sb = new StringBuilder(msgSize * 1024);
        if (messageContentType == 1) {
            message = getRandomMessage(msgSize * 1024);
        } else {
            for (int i = 0; i < msgSize; ++i) {
                sb.append(raw);
            }
            message = sb.toString();
        }

        System.out.printf("chunk打开：%d 压缩打开：%d 大小：%d\n", chunk, compre, msgSize);
        System.out.println("压缩算法:" + zipTypeMap.get(zipType).toString());
        for (int i = 0; i < msgCount; ++i) {
            TimeUnit.MILLISECONDS.sleep(sleepms);
            Long start = System.currentTimeMillis();
            TypedMessageBuilder<byte[]> value = producer.newMessage().value(message.getBytes("UTF-8"));
            System.out.println("-----------------------------");
            System.out.println("消息大小为:" + message.getBytes().length);
            System.out.println("消息大小为:" + message.getBytes().length / 1024 / 1024 + "MB");
            System.out.println("-----------------------------");
            Long end = System.currentTimeMillis();
            System.out.println("========计算压缩消息耗时==============");
            System.out.println("消息压缩耗时:" + (end - start) + "毫秒");
            System.out.println("========压缩消息耗时计算结束===========");
            System.out.printf("发送第%d条： msgId: %s sequenceId %d\n", i + 1, value.send(), i / 10);
        }
        System.out.printf("生产完成 %d  producerName:%s \n", msgCount, producer.getProducerName());
    }

    /**
     * write String from file
     *
     * @param size int
     * @return message String
     */
    private static String getRandomMessage(int size) throws IOException {
        String file = "my-file.txt";
        FileInputStream in = new FileInputStream(new File(file));
        byte[] data = new byte[size];
        in.read(data);
        in.close();
        return new String(data, "UTF-8");
    }
}