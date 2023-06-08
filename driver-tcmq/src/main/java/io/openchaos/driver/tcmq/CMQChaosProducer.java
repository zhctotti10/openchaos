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

import com.qcloud.cmq.Account;
import com.qcloud.cmq.Queue;
import com.qcloud.cmq.Topic;
import com.qcloud.cmq.entity.CmqResponse;
import io.openchaos.common.InvokeResult;
import io.openchaos.driver.queue.QueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CMQChaosProducer implements QueueProducer {

    private static final Logger log = LoggerFactory.getLogger(CMQChaosProducer.class);
    private Queue queue;
    private Topic topic;
    Account account;
    private String chaosTopic;
    private String resourceType;
    private int delaySeconds;
    List<String> vTagList= new ArrayList<>();
    String routingKey;
    String subscriptionType = "tag"; // tag or routingKey

    public CMQChaosProducer(Account account, String chaosTopic, String resourceType, int delaySeconds,
                            List<String> vTagList, String routingKey, String subscriptionType) {
        this.resourceType = resourceType;
        this.chaosTopic = chaosTopic;
        this.account = account;
        this.delaySeconds = delaySeconds;
        this.vTagList = vTagList;
        this.routingKey = routingKey;
        this.subscriptionType = subscriptionType;
    }

    @Override
    public InvokeResult enqueue(byte[] payload) {
        String msg = new String(payload);

        if(CMQDriver.isQueueResType(resourceType)){
            CmqResponse cmqResponse;
            try {
                if(delaySeconds<=0){
                    cmqResponse = queue.send(msg);

                } else {
                    cmqResponse = queue.send(msg,delaySeconds);
                }
            } catch (Exception e) {
                log.warn("Enqueue fail", e);
                return InvokeResult.FAILURE;
            }
            return InvokeResult.SUCCESS.setExtraInfoAndReturnSelf(cmqResponse.getMsgId());
        } else {
            String msgId="";

            try {
                if(isTagSubType(subscriptionType)){
                    if(vTagList == null || vTagList.size()==0){
                        msgId = topic.publishMessage(msg);
                    } else {
                        msgId = topic.publishMessage(msg,vTagList,null);
                    }
                } else {
                    msgId = topic.publishMessage(msg,routingKey);
                }
            } catch (Exception e) {
                log.warn("Enqueue fail", e);
                return InvokeResult.FAILURE;
            }

            return InvokeResult.SUCCESS.setExtraInfoAndReturnSelf(msgId);
        }

    }

    @Override
    public InvokeResult enqueue(String shardingKey, byte[] payload) {
        // Not supported
        return InvokeResult.FAILURE;
    }

    @Override
    public void start() {
        try {
            if(CMQDriver.isQueueResType(resourceType)){
                this.queue = account.getQueue(chaosTopic);

            } else {
                this.topic = account.getTopic(chaosTopic);
            }
        } catch (Exception e) {
            log.error("Failed to start the created producer instance.", e);
        }
    }

    public boolean isTagSubType(String subscriptionType){
        if(subscriptionType.equalsIgnoreCase("routingKey")){
            return false;
        } else {
            return true;
        }
    }

    public void close() {
        // do nothing.
    }
}
