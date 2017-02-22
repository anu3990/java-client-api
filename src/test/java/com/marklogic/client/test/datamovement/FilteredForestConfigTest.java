/*
 * Copyright 2015-2017 MarkLogic Corporation
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
package com.marklogic.client.test.datamovement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.datamovement.FilteredForestConfiguration;
import com.marklogic.client.datamovement.Forest;
import com.marklogic.client.datamovement.ForestConfiguration;
import com.marklogic.client.datamovement.impl.ForestImpl;

public class FilteredForestConfigTest {
  private Logger logger = LoggerFactory.getLogger(FilteredForestConfigTest.class);
  private ForestConfiguration forests = () -> new Forest[] {
      new ForestImpl("host1", "openReplicaHost1", "alternateHost1", "databaseName1",
        "forestName1", "forestId1", true, false),
      new ForestImpl("host2", "openReplicaHost2", null, "databaseName2",
        "forestName2", "forestId2", true, false),
      new ForestImpl("host3", null, "alternateHost3", "databaseName3",
        "forestName3", "forestId3", true, false)
    };

  @Test
  public void testRename() {
    FilteredForestConfiguration ffg = new FilteredForestConfiguration(forests)
      .withRenamedHost("host1", "host1a");

    Forest[] filteredForests = ffg.listForests();

    assertEquals("host1a", filteredForests[0].getHost());
    assertEquals("openreplicahost1", filteredForests[0].getOpenReplicaHost());
    assertEquals("alternatehost1", filteredForests[0].getAlternateHost());
    assertEquals("forestId1", filteredForests[0].getForestId());

    ffg.withRenamedHost("openReplicaHost1", "openReplicaHost1a");

    filteredForests = ffg.listForests();

    assertEquals("host1a", filteredForests[0].getHost());
    assertEquals("openreplicahost1a", filteredForests[0].getOpenReplicaHost());
    assertEquals("alternatehost1", filteredForests[0].getAlternateHost());
    assertEquals("forestId1", filteredForests[0].getForestId());

    ffg.withRenamedHost("alternateHost1", "alternateHost1a");

    filteredForests = ffg.listForests();

    assertEquals("host1a", filteredForests[0].getHost());
    assertEquals("openreplicahost1a", filteredForests[0].getOpenReplicaHost());
    assertEquals("alternatehost1a", filteredForests[0].getAlternateHost());
    assertEquals("forestId1", filteredForests[0].getForestId());
  }

  @Test
  public void testBlackList() {
    FilteredForestConfiguration ffg = new FilteredForestConfiguration(forests)
      .withBlackList("host1")
      .withBlackList("openReplicaHost2")
      .withBlackList("alternateHost3");

    Forest[] filteredForests = ffg.listForests();

    // even though we black-listed "host1", it's only changed if it's the preferredHost
    assertEquals("host1", filteredForests[0].getHost());
    assertEquals("alternatehost1", filteredForests[0].getPreferredHost());
    assertEquals("openreplicahost1", filteredForests[0].getOpenReplicaHost());
    assertEquals("alternatehost1", filteredForests[0].getAlternateHost());
    assertEquals("forestId1", filteredForests[0].getForestId());

    // we black-listed "openReplicaHost2", and it's changed because it's the preferredHost
    assertFalse("openreplicahost2".equals(filteredForests[1].getOpenReplicaHost()));
    assertFalse("openreplicahost2".equals(filteredForests[1].getPreferredHost()));
    assertEquals("host2", filteredForests[1].getHost());
    assertEquals(null, filteredForests[1].getAlternateHost());
    assertEquals("forestId2", filteredForests[1].getForestId());

    // we black-listed "alternateHost3", and it's changed because it's the preferredHost
    assertFalse("alternatehost3".equals(filteredForests[2].getAlternateHost()));
    assertFalse("alternatehost3".equals(filteredForests[2].getPreferredHost()));
    assertEquals("host3", filteredForests[2].getHost());
    assertEquals(null, filteredForests[2].getOpenReplicaHost());
    assertEquals("forestId3", filteredForests[2].getForestId());

  }

  @Test
  public void testWhiteList() {
    FilteredForestConfiguration ffg = new FilteredForestConfiguration(forests)
      .withWhiteList("host1")
      .withWhiteList("openReplicaHost2")
      .withWhiteList("alternateHost3");

    Forest[] filteredForests = ffg.listForests();

    // we white-listed "host1", so it's used, but it's not the preferredHost
    assertEquals("host1", filteredForests[0].getHost());
    // we didn't white-listed "alternateHost1", so it's changed
    assertFalse("alternatehost1".equals(filteredForests[0].getAlternateHost()));
    assertFalse("alternatehost1".equals(filteredForests[0].getPreferredHost()));
    assertFalse("openreplicahost1".equals(filteredForests[0].getOpenReplicaHost()));
    assertEquals("forestId1", filteredForests[0].getForestId());

    // we white-listed "openReplicaHost2", so it's used
    assertEquals("openreplicahost2", filteredForests[1].getOpenReplicaHost());
    assertEquals("openreplicahost2", filteredForests[1].getPreferredHost());
    // and since the preferredHost is white-listed, we left alone the non-preferred hosts
    assertEquals("host2", filteredForests[1].getHost());
    assertEquals(null, filteredForests[1].getAlternateHost());
    assertEquals("forestId2", filteredForests[1].getForestId());

    // we white-listed "alternateHost3", and it's used
    assertEquals("alternatehost3", filteredForests[2].getAlternateHost());
    assertEquals("alternatehost3", filteredForests[2].getPreferredHost());
    // and since the preferredHost is white-listed, we left alone the non-preferred hosts
    assertEquals("host3", filteredForests[2].getHost());
    assertEquals(null, filteredForests[2].getOpenReplicaHost());
    assertEquals("forestId3", filteredForests[2].getForestId());
  }
}