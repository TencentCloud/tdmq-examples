/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tdmq.examples;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
public class DelayedAfterMessageProducer {
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
             *
             *      注意：当使用延迟消息时，必须在 Producer 侧关闭 batch
             */
            Producer<byte[]> producer = pulsarClient.newProducer()
                    .enableBatching(false) // batch 默认是打开的
                    .topic(topicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                /**
                 * 同步发送消息
                 *
                 * 参数说明：
                 *
                 * deliverAfter:
                 *      指定延迟消息的时间，延迟消息的使用有如下两个限制:
                 *        1, 在创建 Producer 时需要关闭 batch (enableBatching(false))
                 *        2, 在创建 Consumer 时必须指定 Shared 的订阅模型
                 *      注意：
                 *          延迟消息的精度为 1s，所以当延迟消息设置的时间为 1s时，broker 在处理时会当作正常消息处理。
                 *          延迟消息最大延迟的时间为 10天，当超过 10天时，延迟消息的发送会报错。
                 */
                MessageId messageId = producer.newMessage()
                        .value("your-delay-message-payload".getBytes(StandardCharsets.UTF_8))
                        .deliverAfter(5, TimeUnit.SECONDS)
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
