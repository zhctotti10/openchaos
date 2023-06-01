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

package io.openchaos.model;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.util.concurrent.RateLimiter;
import io.openchaos.DriverConfiguration;
import io.openchaos.client.Client;
import io.openchaos.client.QueueClient;
import io.openchaos.driver.ChaosNode;
import io.openchaos.driver.queue.QueueDriver;
import io.openchaos.worker.ClientWorker;
import io.openchaos.worker.Worker;
import io.openchaos.common.utils.Utils;
import io.openchaos.recorder.Recorder;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueModel implements Model {
    public static final String MODEL_NAME = "queue";
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Logger log = LoggerFactory.getLogger(QueueModel.class);
    private static final ObjectWriter WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    static {
        MAPPER.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private final List<Client> clients;
    private final List<ClientWorker> workers;
    private final Map<String, ChaosNode> cluster;
    private Map<String, ChaosNode> metaNodesMap;
    private final Recorder recorder;
    private final File driverConfigFile;
    private final int concurrency;
    private final RateLimiter rateLimiter;
    private QueueDriver pubSubDriver;
    private final String chaosTopic;
    private final boolean isOrderTest;
    private final boolean isUsePull;
    private final List<String> shardingKeys;
    private boolean restart;
    private final AtomicLong msgReceivedCount = new AtomicLong(0);

    public QueueModel(int concurrency, RateLimiter rateLimiter, Recorder recorder, File driverConfigFile,
        boolean isOrderTest, boolean isUsePull, List<String> shardingKeys) {
        this.concurrency = concurrency;
        this.recorder = recorder;
        this.driverConfigFile = driverConfigFile;
        this.rateLimiter = rateLimiter;
        clients = new ArrayList<>();
        workers = new ArrayList<>();
        cluster = new HashMap<>();
        metaNodesMap = new HashMap<>();
        chaosTopic = String.format("%s-chaos-topic", DATE_FORMAT.format(new Date()));
        this.isOrderTest = isOrderTest;
        this.isUsePull = isUsePull;
        this.shardingKeys = shardingKeys;
    }

    private static QueueDriver createChaosDriver(File driverConfigFile) throws IOException {

        DriverConfiguration driverConfiguration = MAPPER.readValue(driverConfigFile, DriverConfiguration.class);
        log.info("Initial driver: {}", WRITER.writeValueAsString(driverConfiguration));

        QueueDriver pubSubDriver;

        try {
            pubSubDriver = (QueueDriver) Class.forName(driverConfiguration.driverClass).newInstance();
            pubSubDriver.initialize(driverConfigFile, driverConfiguration.nodes);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return pubSubDriver;
    }

    @Override
    public Map<String, ChaosNode> setupCluster(DriverConfiguration driverConfiguration, boolean isInstall, boolean restart) {
        try {
            if (pubSubDriver == null) {
                pubSubDriver = createChaosDriver(driverConfigFile);
            }

            if (driverConfiguration.metaNodes != null) {
                driverConfiguration.metaNodes.forEach(node -> metaNodesMap.put(node, pubSubDriver.createChaosMetaNode(node, driverConfiguration.metaNodes)));
            }

            if (driverConfiguration.nodes != null) {
                driverConfiguration.nodes.forEach(node -> cluster.put(node, pubSubDriver.createChaosNode(node, driverConfiguration.nodes)));
            }

            if (isInstall) {
                metaNodesMap.values().forEach(ChaosNode::setup);
                cluster.values().forEach(ChaosNode::setup);
            }

            this.restart = restart;
            if (this.restart) {
                log.info("Cluster shutdown");
                cluster.values().forEach(ChaosNode::stop);
                metaNodesMap.values().forEach(ChaosNode::stop);
                log.info("Wait for all nodes to shutdown...");
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                } catch (InterruptedException e) {
                    log.error("", e);
                }

                log.info("Cluster start...");
                log.info("Wait for all nodes to start...");

                metaNodesMap.values().forEach(ChaosNode::start);
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(20));
                } catch (InterruptedException e) {
                    log.error("", e);
                }
                cluster.values().forEach(ChaosNode::start);
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(40));
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }


            if (driverConfiguration.metaNodesParticipateInFault) {
                Map<String, ChaosNode> allNodes = new HashMap<>(metaNodesMap);
                allNodes.putAll(cluster);
                return allNodes;
            } else {
                return cluster;
            }
        } catch (Exception e) {
            log.error("Queue model setupCluster fail", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setupClient() {
        try {
            if (pubSubDriver == null) {
                pubSubDriver = createChaosDriver(driverConfigFile);
            }

            String newTopicName = chaosTopic;
            if(pubSubDriver.useMyTopic()){
                newTopicName = pubSubDriver.myTopic();
                log.info("Reuse existed chaos topic : {}", newTopicName);
            } else {
                log.info("Create chaos topic : {}", chaosTopic);
                pubSubDriver.createTopic(chaosTopic, 8);
            }

            log.info("Clients setup..");

            List<List<String>> shardingKeyLists = Utils.partitionList(shardingKeys, concurrency);
            for (int i = 0; i < concurrency; i++) {
                Client client = new QueueClient(pubSubDriver, newTopicName, recorder, isOrderTest, isUsePull, shardingKeyLists.get(i), msgReceivedCount);
                client.setup();
                clients.add(client);
                ClientWorker clientWorker = new ClientWorker("queueClient-" + i, client, rateLimiter, log);
                workers.add(clientWorker);
            }

            log.info("{} clients setup success", concurrency);
        } catch (Exception e) {
            log.error("Queue model setupClient fail", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        log.info("Start all clients...");
        workers.forEach(Thread::start);
    }

    @Override
    public void stop() {
        log.info("Test stop");
        workers.forEach(Worker::breakLoop);
    }

    @Override
    public void afterStop() {
        clients.forEach(Client::lastInvoke);
    }

    @Override
    public void shutdown() {
        log.info("Teardown client");
        clients.forEach(Client::teardown);
        if (this.restart) {
            log.info("Stop cluster");
            cluster.values().forEach(ChaosNode::stop);
            metaNodesMap.values().forEach(ChaosNode::stop);
        }
        if (pubSubDriver != null) {
            pubSubDriver.shutdown();
        }
    }

    @Override
    public String getStateName() {
        return pubSubDriver.getStateName();
    }    

    @Override
    public boolean probeCluster() {
        int probeTimes = 0;
        try {
            while (msgReceivedCount.get() == 0) {
                if (++probeTimes == 10) {
                    break;
                }
                clients.forEach(Client::nextInvoke);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            log.error("", e);
            return false;
        }
        return probeTimes != 10;
    }
    
    @Override
    public String getMetaNode() {
        return pubSubDriver.getMetaNode();
    }

    @Override
    public String getMetaName() {
        return pubSubDriver.getMetaName();
    }
}
