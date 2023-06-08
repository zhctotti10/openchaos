package io.openchaos.driver.tcmq;/*
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

import com.google.common.collect.Lists;
import com.qcloud.cmq.Account;
import com.qcloud.cmq.Queue;
import com.qcloud.cmq.Topic;
import io.openchaos.common.Message;
import io.openchaos.driver.queue.QueuePullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CMQChaosConsumer implements QueuePullConsumer {

    private static final Logger log = LoggerFactory.getLogger(CMQChaosConsumer.class);
    private Queue queue;
    Account account;
    private String chaosTopic;
    private int waitSec=2;

    public CMQChaosConsumer(Account account, String chaosTopic, int waitSec) {
        this.chaosTopic = chaosTopic;
        this.account = account;
        this.waitSec = waitSec;
    }

    @Override public List<Message> dequeue() {
        int batchSize = 4;
        List<com.qcloud.cmq.Message> messages = new ArrayList<>();
        try{
            if(waitSec<=0){
                messages = queue.batchReceiveMessage(batchSize);
            } else {
                messages = queue.batchReceiveMessage(batchSize,waitSec);
            }
        } catch (Exception e){
            // ignore
        }

        if (!messages.isEmpty()) {
            ArrayList<String> vtReceiptHandle = new ArrayList<String>();
            List<Message> collectList =  new ArrayList<>();
            for(com.qcloud.cmq.Message message: messages){
                vtReceiptHandle.add(message.receiptHandle);
                Message tmpMsg = new Message(message.msgId, message.msgBody.getBytes(),
                        message.enqueueTime, System.currentTimeMillis(), buildConsumeMessageInfo(message));
                collectList.add(tmpMsg);
            }

            try {
                queue.batchDeleteMessage(vtReceiptHandle);
                return collectList;
            } catch (Exception e) {
                log.warn("error happened in ack: ", e);
                return null;
            }
        } else {
            return null;
        }
    }

    public String buildConsumeMessageInfo(com.qcloud.cmq.Message message){
        String msg_info = "";
        msg_info = String.format("msgId:%s, msgBody:%s, requestId:%s, handle:%s", message.msgId, message.msgBody, message.requestId, message.receiptHandle);

        return msg_info;
    }

    @Override public void start() {
        try {
            this.queue = account.getQueue(chaosTopic);
        } catch (Exception e) {
            log.error("Failed to start the created consumer instance.", e);
        }
    }

    @Override public void close() {
        // do nothing.
    }

}
