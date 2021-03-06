/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hamster.protocols.raft.transport;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cluster service implementation.
 */
public class ClusterServiceImpl implements ClusterService {
    /*private ConfigService configService;*/

    private String nodeId;
    private Node localNode;
    private Node controllerNode;
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();

    public ClusterServiceImpl(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public Node getLocalNode() {
        return localNode;
    }

    @Override
    public Node getControllerNode() {
        return controllerNode;
    }

    @Override
    public Collection<Node> getNodes() {
        return nodes.values();
    }

    @Override
    public Node getNode(String id) {
        return nodes.get(id);
    }

    @Override
    public void addNode(Node node) {
        nodes.put(node.id(), node);
        if (node.id().equals(nodeId)) {
            this.localNode = node;
        }
    }


}
