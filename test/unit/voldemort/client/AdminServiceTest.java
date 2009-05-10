/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;

import voldemort.ServerTestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.VoldemortServer.SERVER_STATE;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.utils.Props;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;

/**
 * @author bbansal
 * 
 */
public class AdminServiceTest extends TestCase {

    private static String TEMP_DIR = "test/unit/temp-output";
    private static String storeName = "test-replication-memory";

    VoldemortConfig config;
    VoldemortServer server;
    Cluster cluster;

    @Override
    public void setUp() throws IOException {
        // start 2 node cluster with free ports
        int[] ports = ServerTestUtils.findFreePorts(3);
        Node node0 = new Node(0,
                              "localhost",
                              ports[0],
                              ports[1],
                              ports[2],
                              Arrays.asList(new Integer[] { 0, 1 }));

        ports = ServerTestUtils.findFreePorts(3);
        Node node1 = new Node(1,
                              "localhost",
                              ports[0],
                              ports[1],
                              ports[2],
                              Arrays.asList(new Integer[] { 2, 3 }));

        cluster = new Cluster("admin-service-test", Arrays.asList(new Node[] { node0, node1 }));

        config = createServerConfig(0);
        server = new VoldemortServer(config, cluster);
        server.start();
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        server.stop();
        FileDeleteStrategy.FORCE.delete(new File(TEMP_DIR));
    }

    private VoldemortConfig createServerConfig(int nodeId) throws IOException {
        Props props = new Props();
        props.put("node.id", nodeId);
        props.put("voldemort.home", TEMP_DIR + "/node-" + nodeId);
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        props.put("bdb.write.transactions", "true");
        props.put("bdb.flush.transactions", "true");
        props.put("jmx.enable", "false");
        props.put("enable.mysql.engine", "true");

        config = new VoldemortConfig(props);
        config.setMysqlDatabaseName("voldemort");
        config.setMysqlUsername("voldemort");
        config.setMysqlPassword("voldemort");

        // clean and reinit metadata dir.
        File tempDir = new File(config.getMetadataDirectory());
        tempDir.mkdirs();

        File tempDir2 = new File(config.getDataDirectory());
        tempDir2.mkdirs();

        // copy cluster.xml / stores.xml to temp metadata dir.
        FileUtils.copyFileToDirectory(new File("test/common/voldemort/config/stores.xml"), tempDir);

        return config;
    }

    public void testUpdateCluster() {

        Cluster cluster = server.getMetaDataStore().getCluster();

        // add node 3 and partition 4,5 to cluster.
        ArrayList<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(4);
        partitionList.add(5);
        ArrayList<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        nodes.add(new Node(3, "localhost", 8883, 6668, 7778, partitionList));
        Cluster updatedCluster = new Cluster("new-cluster", nodes);

        // update VoldemortServer cluster.xml
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));

        client.updateClusterMetadata(server.getIdentityNode().getId(),
                                     updatedCluster,
                                     MetadataStore.CLUSTER_KEY);

        assertEquals("Cluster should match", updatedCluster, server.getMetaDataStore().getCluster());
    }

    public void testUpdateOldCluster() {
        Cluster cluster = server.getMetaDataStore().getCluster();

        // add node 3 and partition 4,5 to cluster.
        ArrayList<Integer> partitionList = new ArrayList<Integer>();
        partitionList.add(4);
        partitionList.add(5);
        ArrayList<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        nodes.add(new Node(3, "localhost", 8883, 6668, 7778, partitionList));
        Cluster updatedCluster = new Cluster("new-cluster", nodes);

        // update VoldemortServer cluster.xml
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));

        client.updateClusterMetadata(server.getIdentityNode().getId(),
                                     updatedCluster,
                                     MetadataStore.OLD_CLUSTER_KEY);

        Cluster metaCluster = new ClusterMapper().readCluster(new StringReader(new String(server.getMetaDataStore()
                                                                                                .get(ByteUtils.getBytes(MetadataStore.OLD_CLUSTER_KEY,
                                                                                                                        "UTF-8"))
                                                                                                .get(0)
                                                                                                .getValue())));
        assertEquals("Cluster should match", updatedCluster, metaCluster);

    }

    public void testUpdateStores() {
        List<StoreDefinition> storesList = new ArrayList<StoreDefinition>(server.getMetaDataStore()
                                                                                .getStores());

        // user store should be present
        assertNotSame("StoreDefinition for 'users' should not be nul ",
                      null,
                      server.getMetaDataStore().getStore("users"));

        // remove store users from storesList and update store info.
        int id = -1;
        for(StoreDefinition def: storesList) {
            if(def.getName().equals("users")) {
                id = storesList.indexOf(def);
            }
        }

        if(id != -1) {
            storesList.remove(id);
        }

        // update server stores info
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));

        client.updateStoresMetadata(server.getIdentityNode().getId(), storesList);

        boolean foundUserStore = false;
        for(StoreDefinition def: server.getMetaDataStore().getStores()) {
            if(def.getName().equals("users")) {
                foundUserStore = true;
            }
        }
        assertEquals("Store users should no longer be available", false, foundUserStore);
    }

    public void testRedirectGet() {
        // user store should be present
        Store<ByteArray, byte[]> store = server.getStoreRepository().getStorageEngine("users");

        assertNotSame("Store 'users' should not be null", null, store);

        ByteArray key = new ByteArray(ByteUtils.getBytes("test_member_1", "UTF-8"));
        byte[] value = "test-value-1".getBytes();

        store.put(key, new Versioned<byte[]>(value));

        // check direct get
        assertEquals("Direct Get should succeed", new String(value), new String(store.get(key)
                                                                                     .get(0)
                                                                                     .getValue()));

        // update server stores info
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));

        assertEquals("RedirectGet should match put value",
                     new String(value),
                     new String(client.redirectGet(server.getIdentityNode().getId(), "users", key)
                                      .get(0)
                                      .getValue()));
    }

    public void testRefresh() {
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));
        client.refreshServices(server.getIdentityNode().getId());
    }

    public void testStateTransitions() {
        // change to REBALANCING STATE
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));
        client.changeStateAndRefresh(server.getIdentityNode().getId(),
                                     SERVER_STATE.REBALANCING_STEALER_STATE);

        List<Versioned<byte[]>> values = server.getMetaDataStore()
                                               .get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                       "UTF-8"));
        SERVER_STATE state = SERVER_STATE.valueOf(new String(values.get(0).getValue()));
        assertEquals("State should be changed correctly to rebalancing state",
                     SERVER_STATE.REBALANCING_STEALER_STATE,
                     state);

        // change back to NORMAL state
        client.changeStateAndRefresh(server.getIdentityNode().getId(), SERVER_STATE.NORMAL_STATE);

        values = server.getMetaDataStore().get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                  "UTF-8"));
        state = SERVER_STATE.valueOf(new String(values.get(0).getValue()));
        assertEquals("State should be changed correctly to rebalancing state",
                     SERVER_STATE.NORMAL_STATE,
                     state);

        // lets revert back to REBALANCING STATE AND CHECK (last time I promise
        // :) )
        client.changeStateAndRefresh(server.getIdentityNode().getId(),
                                     SERVER_STATE.REBALANCING_DONOR_STATE);

        values = server.getMetaDataStore().get(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                  "UTF-8"));
        state = SERVER_STATE.valueOf(new String(values.get(0).getValue()));
        assertEquals("State should be changed correctly to rebalancing state",
                     SERVER_STATE.REBALANCING_DONOR_STATE,
                     state);
    }

    public void testGetPartitionsAsStream() {
        // user store should be present
        Store<ByteArray, byte[]> store = server.getStoreRepository().getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);

        // enter keys into server1 (keys 100 -- 1000)
        for(int i = 100; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            store.put(key, new Versioned<byte[]>(value));
        }

        // Get a single partition here
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = client.requestGetPartitionsAsStream(0,
                                                                                                         storeName,
                                                                                                         Arrays.asList(new Integer[] { 0 }));

        StoreDefinition storeDef = server.getMetaDataStore().getStore(storeName);
        assertNotSame("StoreDefinition for 'users' should not be nul ", null, storeDef);
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(server.getCluster()
                                                                              .getNodes(),
                                                                        storeDef.getReplicationFactor());
        // assert all entries are right partitions
        while(entryIterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
            checkEntriesForPartitions(entry.getFirst().get(),
                                      storeName,
                                      new int[] { 0 },
                                      routingStrategy);
        }

        // check for two partitions
        entryIterator = client.requestGetPartitionsAsStream(0,
                                                            storeName,
                                                            Arrays.asList(new Integer[] { 0, 1 }));
        // assert right partitions returned and both are returned
        Set<Integer> partitionSet2 = new HashSet<Integer>();
        while(entryIterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
            checkEntriesForPartitions(entry.getFirst().get(),
                                      storeName,
                                      new int[] { 0, 1 },
                                      routingStrategy);
            partitionSet2.add(routingStrategy.getPartitionList(entry.getFirst().get()).get(0));
        }
        assertEquals("GetPartitionsAsStream should return 2 partitions", 2, partitionSet2.size());
        assertEquals("GetPartitionsAsStream should return {0,1} partitions",
                     true,
                     partitionSet2.contains(new Integer(0))
                             && partitionSet2.contains(new Integer(1)));
    }

    public void testPutEntriesAsStream() throws IOException {
        Store<ByteArray, byte[]> store = server.getStoreRepository().getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);

        ArrayList<Pair<ByteArray, Versioned<byte[]>>> entryList = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();

        // enter keys into server1 (keys 100 -- 1000)
        for(int i = 100; i <= 104; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            entryList.add(Pair.create(key,
                                      Versioned.value(value,
                                                      new VectorClock().incremented(0,
                                                                                    System.currentTimeMillis()))));
        }

        // Write
        AdminClient client = new AdminClient(server.getIdentityNode(),
                                             server.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));

        client.requestPutEntriesAsStream(0, storeName, entryList.iterator());

        for(int i = 100; i <= 104; i++) {
            assertNotSame("Store should return a valid value",
                          "value-" + i,
                          new String(store.get(new ByteArray(ByteUtils.getBytes("" + i, "UTF-8")))
                                          .get(0)
                                          .getValue()));
        }
    }

    public void testPipeGetAndPutStreams() throws IOException {
        Store<ByteArray, byte[]> store = server.getStoreRepository().getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);

        // assert server2 is missing all keys
        for(int i = 100; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            assertEquals("Store should return empty result List for all before inserting",
                         0,
                         store.get(key).size());
        }

        // enter keys into server1 (keys 100 -- 1000)
        for(int i = 100; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            store.put(key,
                      new Versioned<byte[]>(value,
                                            new VectorClock().incremented(0,
                                                                          System.currentTimeMillis())));
        }

        // lets make a new server
        VoldemortConfig config2 = createServerConfig(1);
        VoldemortServer server2 = new VoldemortServer(config2, cluster);
        server2.start();

        // assert server2 is missing all keys
        for(int i = 100; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            assertEquals("Server2 should return empty result List for all",
                         0,
                         server2.getStoreRepository().getStorageEngine(storeName).get(key).size());
        }

        // use pipeGetAndPutStream to add values to server2
        AdminClient client = new AdminClient(server2.getIdentityNode(),
                                             server2.getMetaDataStore(),
                                             new SocketPool(100, 100, 2000, 1000, 10000));

        List<Integer> stealList = new ArrayList<Integer>();
        stealList.add(0);
        stealList.add(1);

        client.pipeGetAndPutStreams(0, 1, storeName, stealList);

        // assert all partition 0, 1 keys present in server 2
        Store<ByteArray, byte[]> store2 = server2.getStoreRepository().getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store2);

        StoreDefinition storeDef = server.getMetaDataStore().getStore(storeName);
        assertNotSame("StoreDefinition for 'users' should not be nul ", null, storeDef);
        RoutingStrategy routingStrategy = new ConsistentRoutingStrategy(server.getCluster()
                                                                              .getNodes(),
                                                                        storeDef.getReplicationFactor());

        int checked = 0;
        int matched = 0;
        for(int i = 100; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + i, "UTF-8");

            if(routingStrategy.getPartitionList(key.get()).get(0) == 0
               || routingStrategy.getPartitionList(key.get()).get(0) == 1) {
                checked++;
                if(store2.get(key).size() > 0
                   && new String(value).equals(new String(store2.get(key).get(0).getValue()))) {
                    matched++;
                }
            }
        }

        server2.stop();
        assertEquals("All Values should have matched", checked, matched);
    }

    private void checkEntriesForPartitions(byte[] key,
                                           String storeName,
                                           int[] partitionList,
                                           RoutingStrategy routingStrategy) {
        Set<Integer> partitionSet = new HashSet<Integer>();
        for(int p: partitionList) {
            partitionSet.add(p);
        }

        int partitionId = routingStrategy.getPartitionList(key).get(0);

        assertEquals("partition:" + partitionId + " should belong to partitionsList:",
                     true,
                     partitionSet.contains(partitionId));

    }
}
