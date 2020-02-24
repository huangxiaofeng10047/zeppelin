/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.notebook;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.user.AuthenticationInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NoteMeta {

  private static Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private String noteId;
  private transient String metaPath;
  private Map<String, Set<String>> permissions = new HashMap<>();

  public NoteMeta(String noteId, AuthenticationInfo subject) {
    this.noteId = noteId;
    initPermissions(subject);
  }

  public void setMetaPath(String metaPath) {
    this.metaPath = metaPath;
  }

  // used when creating new note
  public void initPermissions(AuthenticationInfo subject) {
    if (!AuthenticationInfo.isAnonymous(subject)) {
      if (ZeppelinConfiguration.create().isNotebookPublic()) {
        // add current user to owners - can be public
        Set<String> owners = getOwners();
        owners.add(subject.getUser());
        setOwners(owners);
      } else {
        // add current user to owners, readers, runners, writers - private note
        Set<String> entities = getOwners();
        entities.add(subject.getUser());
        setOwners(entities);
        entities = getReaders();
        entities.add(subject.getUser());
        setReaders(entities);
        entities = getRunners();
        entities.add(subject.getUser());
        setRunners(entities);
        entities = getWriters();
        entities.add(subject.getUser());
        setWriters(entities);
      }
    }
  }

  public String getNoteId() {
    return noteId;
  }

  public void setOwners(Set<String> entities) {
    permissions.put("owners", entities);
  }

  public Set<String> getOwners() {
    Set<String> owners = permissions.get("owners");
    if (owners == null) {
      owners = new HashSet<>();
    } else {
      owners = checkCaseAndConvert(owners);
    }
    return owners;
  }

  public Set<String> getReaders() {
    Set<String> readers = permissions.get("readers");
    if (readers == null) {
      readers = new HashSet<>();
    } else {
      readers = checkCaseAndConvert(readers);
    }
    return readers;
  }

  public void setReaders(Set<String> entities) {
    permissions.put("readers", entities);
  }

  public Set<String> getRunners() {
    Set<String> runners = permissions.get("runners");
    if (runners == null) {
      runners = new HashSet<>();
    } else {
      runners = checkCaseAndConvert(runners);
    }
    return runners;
  }

  public void setRunners(Set<String> entities) {
    permissions.put("runners", entities);
  }

  public Set<String> getWriters() {
    Set<String> writers = permissions.get("writers");
    if (writers == null) {
      writers = new HashSet<>();
    } else {
      writers = checkCaseAndConvert(writers);
    }
    return writers;
  }

  public void setWriters(Set<String> entities) {
    permissions.put("writers", entities);
  }

  /*
   * If case conversion is enforced, then change entity names to lower case
   */
  private Set<String> checkCaseAndConvert(Set<String> entities) {
    if (ZeppelinConfiguration.create().isUsernameForceLowerCase()) {
      Set<String> set2 = new HashSet<String>();
      for (String name : entities) {
        set2.add(name.toLowerCase());
      }
      return set2;
    } else {
      return entities;
    }
  }

  public String toJson() {
    return GSON.toJson(this);
  }

  public static NoteMeta fromJson(String json) {
    return GSON.fromJson(json, NoteMeta.class);
  }
}
