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

package voldemort.server.socket;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortService;
import voldemort.server.protocol.RequestHandler;

/**
 * The VoldemortService that loads up the socket server
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "A server that handles remote operations on stores via tcp/ip.")
public class SocketService extends AbstractService implements VoldemortService {

    private final SocketServer server;

    public SocketService(RequestHandler requestHandler,
                         int port,
                         int coreConnections,
                         int maxConnections,
                         int socketBufferSize,
                         String serviceName) {
        super(ServiceType.SOCKET);
        this.server = new SocketServer(serviceName,
                                       port,
                                       coreConnections,
                                       maxConnections,
                                       socketBufferSize,
                                       requestHandler);
    }

    @Override
    protected void startInner() {
        this.server.start();
        this.server.awaitStartupCompletion();
    }

    @Override
    protected void stopInner() {
        this.server.shutdown();
    }

    @JmxGetter(name = "port", description = "The port on which the server is accepting connections.")
    public int getPort() {
        return server.getPort();
    }

    public StatusManager getStatusManager() {
        return server.getStatusManager();
    }
}