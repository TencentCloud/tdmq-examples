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

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

@Slf4j
public class SyncSimpleConsumer {
    public static void main(String[] args) {
        // 集群服务接入地址, 可以在控制台【集群管理】页面查看并复制。
        final String brokerServiceUrl = "";
        // 角色密钥，在【角色管理】页面复制密钥列复制。
        final String tokenString = "";
        // topic 完整路径，格式为 persistent://集群（租户）ID/命名空间/Topic名称
        final String topicName = "persistent://your-tenant/your-ns/your-topic";
        // 订阅的名字
        final String subName = "your-sub";
        final int numMessages = 10;

        try {
            // 创建 PulsarClient 用于连接集群
            PulsarClient pulsarClient = PulsarClient.builder()
                    .serviceUrl(brokerServiceUrl)
                    .authentication(AuthenticationFactory.token(tokenString))
                    .build();

            /**
             * 创建 []byte 类型的 Consumer。
             *
             * 参数说明：
             *     subscriptionName: 可以从管控台创建，也可以在代码中指定。如果代码中指定即 Broker 会自动创建该订阅
             *     subscriptionType: 订阅的类型, 当前 TDMQ 支持如下订阅类型
             *          Failover: 保证消息的顺序，同一时刻只有一个活跃的 Consumer 在线消费消息，其它 Consumer 作为备 Consumer，当活跃的 Consumer 挂掉之后可以继续消费。
             *          Exclusive: 保证消息的顺序，只允许创建一个 Consumer。（默认值）
             *          Shared: 不保证消息消费的顺序，同一时刻允许有多个 Consumer 同时消费消息
             *          Key_Shared: 基于消息 Key 保证消息消费的顺序，可以保证同一个 Key 的消息，只分发给同一个 Consumer 来消费
             *     subscriptionInitialPosition: 消息起始消费的位置，默认从 Latest 位置消费，TDMQ 支持如下两种起始消费位置：
             *          Latest: 从 Consumer 注册上来的那个时间点开始消费，会忽略 Consumer 注册上来之前的消息
             *          Earliest: 从发送到该 Topic 中最早的没有被确认的位置开始消费，随着消息在 Topic 中不断的被 acknowledgeAsync，
             *                  Earliest 的位置也会不断的变化。即 Earliest 的位置指向当前 Topic 最早没被确认的位置。
             *
             */
            Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subName)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();

            for (int i = 0; i < numMessages; i++) {
                Message<byte[]> msg = consumer.receive(500, TimeUnit.MILLISECONDS);
                if (null != msg) {
                    byte[] data = msg.getValue();
                    System.out.println(
                            "Receive messageID " + msg.getMessageId() + " message payload: " + new String(data));
                    consumer.acknowledgeAsync(msg);
                }
            }

            consumer.close();
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.error("Receive message error: ", e);
        }
    }
}
