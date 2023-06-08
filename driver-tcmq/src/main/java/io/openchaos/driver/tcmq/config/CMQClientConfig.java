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

package io.openchaos.driver.tcmq.config;

public class CMQClientConfig {
    public String serviceURL;
    public String queueName;
    public String topicName;
    public String resourceType="queue";
    public int delaySeconds;
    public int waitSeconds = 2;
    public String subscriptionType = "tag";
    public String routingKey;
    public String tags;
    public String secretId;
    public String secretKey;
}
