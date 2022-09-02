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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

@Slf4j
public class AsyncSimpleConsumer {
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

            Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName(subName)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();

            for (int i = 0; i < numMessages; i++) {
                consumer.receiveAsync()
                        .whenComplete((msg, cause) -> {
                            if (null == cause) {
                                byte[] data = msg.getValue();
                                System.out.println(
                                        "Receive messageID " + msg.getMessageId() + " message payload: " + new String(
                                                data));
                                consumer.acknowledgeAsync(msg);
                            } else {
                                System.err.println("Failed to receive message : ");
                                cause.printStackTrace();
                            }
                        });
            }

            consumer.close();
            pulsarClient.close();
        } catch (PulsarClientException e) {
            log.error("Receive message error: ", e);
        }
    }
}
