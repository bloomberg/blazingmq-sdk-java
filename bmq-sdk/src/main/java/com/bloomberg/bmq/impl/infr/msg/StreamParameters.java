/*
 * Copyright 2022 Bloomberg Finance L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bloomberg.bmq.impl.infr.msg;

import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.SubscriptionExpression;
import java.util.Map;

public class StreamParameters {

    public static final int CONSUMER_PRIORITY_INVALID = Integer.MIN_VALUE;
    public static final String DEFAULT_APP_ID = "__default";

    private String appId;
    private Subscription[] subscriptions;

    public StreamParameters() {
        init();
    }

    public void reset() {
        init();
    }

    private void init() {
        appId = DEFAULT_APP_ID;
        subscriptions = new Subscription[] {};
    }

    public String appId() {
        return appId;
    }

    public void setAppId(String value) {
        appId = value;
    }

    public Subscription[] subscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(Subscription[] value) {
        subscriptions = value;
    }

    public static StreamParameters createDeconfigureParameters(SubQueueIdInfo subQueueIdInfo) {
        StreamParameters params = new StreamParameters();

        if (subQueueIdInfo != null) {
            params.setAppId(subQueueIdInfo.appId());
        }

        // Empty subscriptions for deconfigure

        return params;
    }

    public static StreamParameters createParameters(
            SubQueueIdInfo subQueueIdInfo, QueueOptions ops) {
        int consumerPriorityCount = 1;

        StreamParameters params = new StreamParameters();

        if (subQueueIdInfo != null) {
            params.setAppId(subQueueIdInfo.appId());
        }

        Subscription[] subscriptions;

        if (ops.getSubscriptions().size() == 0) {
            Subscription sn = new Subscription();

            sn.setId(com.bloomberg.bmq.Subscription.nextId());

            ConsumerInfo consumer = new ConsumerInfo();
            consumer.setConsumerPriority(ops.getConsumerPriority());
            consumer.setConsumerPriorityCount(consumerPriorityCount);
            consumer.setMaxUnconfirmedMessages(ops.getMaxUnconfirmedMessages());
            consumer.setMaxUnconfirmedBytes(ops.getMaxUnconfirmedBytes());
            sn.setConsumers(new ConsumerInfo[] {consumer});

            Expression expr = new Expression();
            expr.setText(SubscriptionExpression.k_EXPRESSION_DEFAULT);
            expr.setVersion(SubscriptionExpression.k_VERSION_DEFAULT.toString().toUpperCase());
            sn.setExpression(expr);

            subscriptions = new Subscription[] {sn};
        } else {
            subscriptions = new Subscription[ops.getSubscriptions().size()];
            int index = 0;

            for (Map.Entry<Integer, com.bloomberg.bmq.Subscription> entry :
                    ops.getSubscriptions().entrySet()) {
                com.bloomberg.bmq.Subscription snConfig = entry.getValue();

                Subscription sn = new Subscription();

                sn.setOrigin(snConfig);
                sn.setId(entry.getKey());

                ConsumerInfo consumer = new ConsumerInfo();
                consumer.setConsumerPriority(
                        snConfig.hasConsumerPriority()
                                ? snConfig.getConsumerPriority()
                                : ops.getConsumerPriority());
                consumer.setConsumerPriorityCount(consumerPriorityCount);
                consumer.setMaxUnconfirmedMessages(
                        snConfig.hasMaxUnconfirmedMessages()
                                ? snConfig.getMaxUnconfirmedMessages()
                                : ops.getMaxUnconfirmedMessages());
                consumer.setMaxUnconfirmedBytes(
                        snConfig.hasMaxUnconfirmedBytes()
                                ? snConfig.getMaxUnconfirmedBytes()
                                : ops.getMaxUnconfirmedBytes());
                sn.setConsumers(new ConsumerInfo[] {consumer});

                Expression expr = new Expression();
                expr.setText(snConfig.getExpression().getExpression());
                expr.setVersion(snConfig.getExpression().getVersion().toString().toUpperCase());
                sn.setExpression(expr);

                subscriptions[index++] = sn;
            }
        }

        params.setSubscriptions(subscriptions);
        return params;
    }
}