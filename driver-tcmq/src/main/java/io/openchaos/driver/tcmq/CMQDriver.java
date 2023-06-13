/*
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

package io.openchaos.driver.tcmq;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.qcloud.cmq.Account;
import io.openchaos.driver.MetaNode;
import io.openchaos.driver.queue.*;
import io.openchaos.driver.tcmq.config.CMQClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class CMQDriver implements QueueDriver {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Logger log = LoggerFactory.getLogger(CMQDriver.class);
    private CMQClientConfig rmqClientConfig;


    private static CMQClientConfig readConfigForClient(File configurationFile) throws IOException {
        return MAPPER.readValue(configurationFile, CMQClientConfig.class);
    }


    @Override
    public void initialize(File configurationFile, List<String> nodes) throws IOException {
        this.rmqClientConfig = readConfigForClient(configurationFile);
    }

    @Override
    public QueueNode createChaosNode(String node, List<String> nodes) {
        return new CMQChaosNode();
    }

    @Override
    public QueueProducer createProducer(String topic) {
        Account account = new Account(rmqClientConfig.serviceURL, rmqClientConfig.secretId, rmqClientConfig.secretKey);

        List<String> vTagList = new ArrayList<>();
        if(rmqClientConfig.tags !=null && !rmqClientConfig.tags.isEmpty()){
            vTagList = Arrays.asList(rmqClientConfig.tags.replace(" ", "").split(","));
        }
        return new CMQChaosProducer(account, topic, rmqClientConfig.resourceType, rmqClientConfig.delaySeconds, vTagList,
                rmqClientConfig.routingKey, rmqClientConfig.subscriptionType);
    }

    @Override
    public String subscriptionName(){
        return "ChaosTest_ConsumerGroup";
    }

    @Override
    public QueuePushConsumer createPushConsumer(String topic, String subscriptionName,
        ConsumerCallback consumerCallback) {

        return null;
    }

    @Override
    public QueuePullConsumer createPullConsumer(String topic, String subscriptionName) {
        Account account = new Account(rmqClientConfig.serviceURL, rmqClientConfig.secretId, rmqClientConfig.secretKey);

        return new CMQChaosConsumer(account, rmqClientConfig.queueName, rmqClientConfig.waitSeconds, rmqClientConfig.ackDelayInMs);
    }

    @Override
    public String getMetaNode() {
        return "";
    }

    @Override
    public String getMetaName() {
        return "";
    }

    @Override
    public void createTopic(String topic, int partitions) {
        // do nothing
    }

    public void shutdown() {

    }

    @Override public MetaNode createChaosMetaNode(String node, List<String> nodes) {
        return new CMQMetaNode();
    }

    @Override
    public String getStateName() {
        return "io.openchaos.driver.CMQ.CMQChaosState";
    }

    @Override
    public boolean useMyTopic() {
        return true;
    }

    @Override
    public String myTopic() {
        if(isQueueResType(rmqClientConfig.resourceType)){
            return rmqClientConfig.queueName;
        } else {
            return rmqClientConfig.topicName;
        }

    }

    public static boolean isQueueResType(String resourceType){
        if(resourceType.equalsIgnoreCase("queue")){
            return true;
        } else {
            return false;
        }
    }

}
