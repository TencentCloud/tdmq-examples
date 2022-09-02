package org.tdmq.examples;

import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.Murmur3_32Hash;

@Slf4j
public class SyncProducerWithMessageRoute {
    public static void main(String[] args) {
        // 集群服务接入地址, 可以在控制台【集群管理】页面查看并复制。
        final String brokerServiceUrl = "";
        // 角色密钥，在【角色管理】页面复制密钥列复制。
        final String tokenString = "";
        // topic 完整路径，格式为 persistent://集群（租户）ID/命名空间/Topic名称
        final String topicName = "";
        final int numMessages = 10;

        try {
            // 创建 PulsarClient 用于连接集群
            PulsarClient pulsarClient = PulsarClient.builder()
                    .serviceUrl(brokerServiceUrl)
                    .authentication(AuthenticationFactory.token(tokenString))
                    .build();

            /**
             * 构建 byte[] 类型的生产者用于生产消息
             *
             * 参数说明：
             *
             * enableBatching:
             *      batch 默认是打开的，当开启 batch 时，消息轨迹页面查询消息时，只可以查询 batch 中的第一条消息。
             *      如果想要关闭 batch，需要显示设置，可以参考如下代码：enableBatching(false) 。
             * messageRouter:
             *      自定义消息路由的模型，可以重写 choosePartition() 接口
             */
            Producer<byte[]> producer = pulsarClient.newProducer()
                    .enableBatching(false)
                    .topic(topicName)
                    .messageRouter(new MessageRouter() {
                        @Override
                        public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                            return Murmur3_32Hash.getInstance().makeHash(msg.getKeyBytes()) % metadata.numPartitions();
                        }
                    })
                    .create();

            for (int i = 0; i < numMessages; i++) {
                /**
                 * 同步发送消息
                 *
                 * 参数说明：
                 *
                 * key:
                 *      可选参数，可以不指定，当不指定消息的 key 时，消息默认按照 round robin 的形式路由到不同的 partition 中；
                 *      当指定消息 key 的时候，消息按照 key hash 的形式路由到不同的 partitions 中；当用户自定义 messageRouter 时，
                 *      按照用户自定义的规则进行消息的路由。
                 */
                MessageId messageId = producer.newMessage()
                        .key("your-message-key")
                        .value("your-message-payload".getBytes(StandardCharsets.UTF_8))
                        .send();

                System.out.println("The publish messageID: " + messageId);
            }

            // flush all outstanding requests
            producer.flush();

            // 关闭创建出来的资源
            producer.close();
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.error("Send message error: ", e);
        }
    }
}
