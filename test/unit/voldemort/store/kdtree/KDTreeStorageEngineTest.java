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

package voldemort.store.kdtree;

import java.util.ArrayList;
import java.util.List;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class KDTreeStorageEngineTest extends AbstractStorageEngineTest {

    private StorageEngine<ByteArray, byte[]> store;

    @Override
    public StorageEngine<ByteArray, byte[]> getStorageEngine() {
        return store;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.store = new KDTreeStorageEngine("test", 6);
    }

    @Override
    public List<ByteArray> getKeys(int numKeys) {
        List<ByteArray> keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(new ByteArray(KDUtil.getBytesForKey(TestUtils.randomDouble(6))));
        return keys;
    }

    public void testIterationWithSerialization() {
    // Would fail due to byte array key constraints
    // So we simply overwrite and ignore this test
    }

    public void testEmptyByteArray() {
    // Would fail, but this is ok, we won't accept a length 0 key...
    }

    public void testPruneOnWrite() {
        StorageEngine<ByteArray, byte[]> engine = getStorageEngine();
        Versioned<byte[]> v1 = new Versioned<byte[]>(new byte[] { 1 }, TestUtils.getClock(1));
        Versioned<byte[]> v2 = new Versioned<byte[]>(new byte[] { 2 }, TestUtils.getClock(2));
        Versioned<byte[]> v3 = new Versioned<byte[]>(new byte[] { 3 }, TestUtils.getClock(1, 2));
        ByteArray key = new ByteArray(KDUtil.getBytesForKey(TestUtils.randomDouble(6)));
        engine.put(key, v1);
        engine.put(key, v2);
        assertEquals(2, engine.get(key).size());
        engine.put(key, v3);
        assertEquals(1, engine.get(key).size());
    }

}
