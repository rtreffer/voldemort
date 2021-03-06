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

package voldemort.client.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;

/**
 * The client side of the protocol buffers request format
 * 
 * @author jay
 * 
 */
public class ProtoBuffClientRequestFormat implements RequestFormat {

    public final ErrorCodeMapper mapper;

    public ProtoBuffClientRequestFormat() {
        this.mapper = new ErrorCodeMapper();
    }

    public void writeDeleteRequest(DataOutputStream output,
                                   String storeName,
                                   ByteArray key,
                                   VectorClock version,
                                   boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        VProto.VoldemortRequest.newBuilder()
                               .setType(RequestType.DELETE)
                               .setStore(storeName)
                               .setShouldRoute(shouldReroute)
                               .setDelete(VProto.DeleteRequest.newBuilder()
                                                              .setKey(ByteString.copyFrom(key.get()))
                                                              .setVersion(ProtoUtils.encodeClock(version)))
                               .build()
                               .writeTo(output);
    }

    public boolean readDeleteResponse(DataInputStream input) throws IOException {
        VProto.DeleteResponse response = VProto.DeleteResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
        return response.getSuccess();
    }

    public void writeGetRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        VProto.VoldemortRequest.newBuilder()
                               .setType(RequestType.GET)
                               .setStore(storeName)
                               .setShouldRoute(shouldReroute)
                               .setGet(VProto.GetRequest.newBuilder()
                                                        .setKey(ByteString.copyFrom(key.get())))
                               .build()
                               .writeTo(output);
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream input) throws IOException {
        VProto.GetResponse response = VProto.GetResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
        return ProtoUtils.decodeVersions(response.getVersionedList());
    }

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> keys,
                                   boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKeys(keys);
        VProto.GetAllRequest.Builder req = VProto.GetAllRequest.newBuilder();
        for(ByteArray key: keys)
            req.addKeys(ByteString.copyFrom(key.get()));

        VProto.VoldemortRequest.newBuilder()
                               .setType(RequestType.GET_ALL)
                               .setStore(storeName)
                               .setShouldRoute(shouldReroute)
                               .setGetAll(req)
                               .build()
                               .writeTo(output);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream input)
            throws IOException {
        VProto.GetAllResponse response = VProto.GetAllResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
        Map<ByteArray, List<Versioned<byte[]>>> vals = new HashMap<ByteArray, List<Versioned<byte[]>>>(response.getValuesCount());
        for(VProto.KeyedVersions versions: response.getValuesList())
            vals.put(ProtoUtils.decodeBytes(versions.getKey()),
                     ProtoUtils.decodeVersions(versions.getVersionsList()));
        return vals;
    }

    public void writePutRequest(DataOutputStream output,
                                String storeName,
                                ByteArray key,
                                byte[] value,
                                VectorClock version,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        VProto.PutRequest.Builder req = VProto.PutRequest.newBuilder()
                                                         .setKey(ByteString.copyFrom(key.get()))
                                                         .setVersioned(VProto.Versioned.newBuilder()
                                                                                       .setValue(ByteString.copyFrom(value))
                                                                                       .setVersion(ProtoUtils.encodeClock(version)));
        VProto.VoldemortRequest.newBuilder()
                               .setType(RequestType.PUT)
                               .setStore(storeName)
                               .setShouldRoute(shouldReroute)
                               .setPut(req)
                               .build()
                               .writeTo(output);
    }

    public void readPutResponse(DataInputStream input) throws IOException {
        VProto.PutResponse response = VProto.PutResponse.parseFrom(input);
        if(response.hasError())
            throwException(response.getError());
    }

    public void throwException(VProto.Error error) {
        throw mapper.getError((short) error.getErrorCode(), error.getErrorMessage());
    }

}
