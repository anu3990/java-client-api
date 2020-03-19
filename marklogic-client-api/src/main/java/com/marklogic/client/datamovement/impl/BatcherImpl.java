/*
 * Copyright (c) 2019 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.client.datamovement.impl;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BatcherImpl implements Batcher {
  private String jobName = "unnamed";
  private String jobId = null;
  private int batchSize = 100;
  private int threadCount = 1;
  private ForestConfiguration forestConfig;
  private DataMovementManagerImpl moveMgr;
  private static Logger logger = LoggerFactory.getLogger(BatcherImpl.class);
  Map<String, WriteBatcherImpl.HostInfo> existingHostInfos = new HashMap<>();
  Map<String, WriteBatcherImpl.HostInfo> removedHostInfos = new HashMap<>();

  protected BatcherImpl(DataMovementManager moveMgr){
    if (moveMgr == null)
      throw new IllegalArgumentException("moveMgr must not be null");
    if (!(moveMgr instanceof DataMovementManagerImpl))
      throw new IllegalArgumentException("moveMgr must be DataMovementManagerImpl");
    this.moveMgr = (DataMovementManagerImpl) moveMgr;
  }

  @Override
  public Batcher withJobName(String jobName) {
    this.jobName = jobName;
    return this;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  @Override
  public String getJobName() {
    return jobName;
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  @Override
  public Batcher withBatchSize(int batchSize) {
    if ( batchSize <= 0 ) {
      throw new IllegalArgumentException("batchSize must be 1 or greater");
    }
    this.batchSize = batchSize;
    return this;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public Batcher withThreadCount(int threadCount) {
    if ( threadCount <= 0 ) {
      throw new IllegalArgumentException("threadCount must be 1 or greater");
    }
    this.threadCount = threadCount;
    return this;
  }

  @Override
  public int getThreadCount() {
    return threadCount;
  }

  @Override
  public ForestConfiguration getForestConfig() {
    return forestConfig;
  }

  @Override
  public Batcher withForestConfig(ForestConfiguration forestConfig) {
    if ( forestConfig == null ) throw new IllegalArgumentException("forestConfig must not be null");
    if (moveMgr.getConnectionType() == DatabaseClient.ConnectionType.GATEWAY && !(forestConfig instanceof ForestConfigurationImpl))
      throw new IllegalArgumentException("cannot change internal forestConfig when using a gateway");
    this.forestConfig = forestConfig;
    return this;
  }

  @Override
  public DatabaseClient getPrimaryClient() {
    return getMoveMgr().getPrimaryClient();
  }

  public abstract void start(JobTicket ticket);
  public abstract JobTicket getJobTicket();
  public abstract void stop();

  @Override
  public abstract boolean isStopped();

  @Override
  public abstract boolean isStarted();

  protected DataMovementManagerImpl getMoveMgr() {
    return moveMgr;
  }

  protected Forest[] forests(ForestConfiguration config) {
    if (config == null) {
      throw new IllegalArgumentException("forestConfig must not be null");
    }
    return config.listForests();
  }
  protected Set<String> hosts(Forest[] forests) {
    if (forests.length == 0) {
      throw new IllegalStateException("batcher requires at least one forest");
    }
    Set<String> hosts = new HashSet<>();
    for (Forest forest: forests) {
      if (forest.getPreferredHost() == null) {
        throw new IllegalStateException("Hostname must not be null for any forest");
      }
      hosts.add(forest.getPreferredHost());
    }
    for (Forest forest: forests) {
      String hostName = forest.getHost();
      if (forest.getPreferredHostType() == Forest.HostType.REQUEST_HOST &&
              !hostName.toLowerCase().equals(forest.getRequestHost().toLowerCase())) {
        if (hosts.contains(hostName))
          hosts.remove(hostName);
      }
    }
    return hosts;
  }
  protected List<DatabaseClient> clients(Set<String> hosts) {
    if (hosts == null || hosts.size() == 0) {
      throw new IllegalStateException("no hosts for batcher");
    }
    List<DatabaseClient> clients = new ArrayList<>();
    for (String host: hosts) {
      clients.add(moveMgr.getHostClient(host));
    }
    return clients;
  }

  WriteBatcherImpl.HostInfo[] forestHosts(ForestConfiguration forestConfig, WriteBatcherImpl.HostInfo[] hostInfos) {
    // get the list of hosts to use
    Forest[] forests = forests(forestConfig);
    Set<String> hosts = hosts(forests);

    if (hostInfos != null) {
      for (WriteBatcherImpl.HostInfo hostInfo : hostInfos) {
        existingHostInfos.put(hostInfo.hostName, hostInfo);
        removedHostInfos.put(hostInfo.hostName, hostInfo);
      }
    }
    logger.info("(withForestConfig) Using forests on {} hosts for \"{}\"", hosts, forests[0].getDatabaseName());
    // initialize a DatabaseClient for each host
    WriteBatcherImpl.HostInfo[] newHostInfos = new WriteBatcherImpl.HostInfo[hosts.size()];
    int i = 0;
    for (String host : hosts) {
      if (existingHostInfos.get(host) != null) {
        newHostInfos[i] = existingHostInfos.get(host);
        removedHostInfos.remove(host);
      } else {
        newHostInfos[i] = new WriteBatcherImpl.HostInfo();
        newHostInfos[i].hostName = host;
        // this is a host-specific client (no DatabaseClient is actually forest-specific)
        newHostInfos[i].client = getMoveMgr().getHostClient(host);
        if (getMoveMgr().getConnectionType() == DatabaseClient.ConnectionType.DIRECT) {
          logger.info("Adding DatabaseClient on port {} for host \"{}\" to the rotation",
                  newHostInfos[i].client.getPort(), host);
        }
      }
      i++;
    }
    this.forestConfig = forestConfig;

    return newHostInfos;
  }

}
