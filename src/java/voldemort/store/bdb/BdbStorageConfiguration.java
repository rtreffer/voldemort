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

package voldemort.store.bdb;

import java.io.File;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;
import voldemort.store.StorageInitializationException;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;

import com.google.common.collect.Maps;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * The configuration that is shared between berkeley db instances. This includes
 * the db environment and the configuration
 * 
 * @author jay
 * 
 */
public class BdbStorageConfiguration implements StorageConfiguration {

    private static Logger logger = Logger.getLogger(BdbStorageConfiguration.class);

    private Environment environment;
    private EnvironmentConfig environmentConfig;
    private DatabaseConfig databaseConfig;
    private boolean isInitialized = false;
    private Map<String, BdbStorageEngine> stores = Maps.newHashMap();

    public BdbStorageConfiguration(VoldemortConfig config) {
        try {
            environmentConfig = new EnvironmentConfig();
            environmentConfig.setTransactional(true);
            environmentConfig.setCacheSize(config.getBdbCacheSize());
            if(config.isBdbWriteTransactionsEnabled() && config.isBdbFlushTransactionsEnabled()) {
                environmentConfig.setTxnNoSync(false);
                environmentConfig.setTxnWriteNoSync(false);
            } else if(config.isBdbWriteTransactionsEnabled()
                      && !config.isBdbFlushTransactionsEnabled()) {
                environmentConfig.setTxnNoSync(false);
                environmentConfig.setTxnWriteNoSync(true);
            } else {
                environmentConfig.setTxnNoSync(true);
            }
            environmentConfig.setAllowCreate(true);
            environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
                                             Long.toString(config.getBdbMaxLogFileSize()));
            environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
                                             Long.toString(config.getBdbCheckpointBytes()));
            environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL,
                                             Long.toString(config.getBdbCheckpointMs()
                                                           * Time.US_PER_MS));
            databaseConfig = new DatabaseConfig();
            databaseConfig.setAllowCreate(true);
            databaseConfig.setSortedDuplicates(config.isBdbSortedDuplicatesEnabled());
            databaseConfig.setNodeMaxEntries(config.getBdbBtreeFanout());
            databaseConfig.setTransactional(true);
            File bdbDir = new File(config.getBdbDataDirectory());
            if(!bdbDir.exists()) {
                logger.info("Creating BDB data directory '" + bdbDir.getAbsolutePath() + "'.");
                bdbDir.mkdirs();
            }
            environment = new Environment(bdbDir, environmentConfig);
            isInitialized = true;
        } catch(DatabaseException e) {
            throw new StorageInitializationException(e);
        }
    }

    public synchronized StorageEngine<ByteArray, byte[]> getStore(String storeName) {
        if(!isInitialized)
            throw new StorageInitializationException("Attempt to get store for uninitialized storage configuration!");

        if(stores.containsKey(storeName)) {
            return stores.get(storeName);
        } else {
            try {
                Database db = environment.openDatabase(null, storeName, databaseConfig);
                BdbStorageEngine engine = new BdbStorageEngine(storeName, environment, db);
                stores.put(storeName, engine);
                return engine;
            } catch(DatabaseException d) {
                throw new StorageInitializationException(d);
            }
        }
    }

    public StorageEngineType getType() {
        return StorageEngineType.BDB;
    }

    public void close() {
        try {
            this.environment.sync();
            this.environment.close();
        } catch(DatabaseException e) {
            throw new VoldemortException(e);
        }
    }

}
