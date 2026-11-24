---
title: "Processor State in Apache NiFi"
---
## Introduction

Some Apache NiFi processors need to "remember" information between runs. For example, `ListSFTP` records which file was processed last so that it does not read it again, even after a restart. This information is called **state**.

State is stored in a dedicated mechanism, the **State Management Provider**, and is unrelated to FlowFile attributes or the contents of NiFi repositories. It comes in two kinds: local (on a specific node) and cluster-wide (shared by all nodes).

This summary covers where state is stored, where it is configured, what determines its scope, and how to view it.

## 1. Where Processor State Is Stored

In NiFi, some processors have state. It is stored in the **State Management Provider**, a dedicated mechanism for managing component state.

State is **not the same** as:
- FlowFile attributes;
- the contents of NiFi repositories.

### Two Main Storage Options
- **Local State**: stored locally on the file system of a specific node.
- **Cluster State**: stored centrally and available to all nodes in the cluster.

The choice depends on the processor's configuration and implementation.

### Where to Look

| State Type | Where It Is Stored |
|---|---|
| **Local** | `$NIFI_HOME/state/local` |
| **Cluster** | In ZooKeeper (by default) |
| **Cluster in Kubernetes** | In Kubernetes ConfigMaps (if the corresponding provider is configured) |

## 2. Where It Is Configured

Configuration is located in two files.

### `conf/nifi.properties`
```
nifi.state.management.configuration.file=./conf/state-management.xml

nifi.state.management.provider.local=local-provider
nifi.state.management.provider.cluster=zk-provider
```

### `conf/state-management.xml`
This file describes the specific State Provider implementations, for example:
- `WriteAheadLocalStateProvider` (for local state);
- `ZooKeeperStateProvider` (for cluster state).

## 3. Important Nuance: The Processor Determines the Scope

The processor itself decides which scope to use:
- **`Scope.LOCAL`**: separate state for each node.
- **`Scope.CLUSTER`**: shared state for the entire cluster.

State is **isolated per component**: two instances of the same processor do not automatically share it.

**Example:** `ListSFTP` may store information about the last processed file so that it does not read it again after a restart.

More details: Apache NiFi documentation, *State Management* section.

## 4. Can You Inspect a Processor's State?

Yes, there are several ways.

### Method 1. Via the UI
1. Right-click the processor.
2. Select **View state**.
3. NiFi displays the keys and values stored by the processor.
4. The state scope (Local or Cluster) is also shown there.

### Method 2. Via the REST API
Get the state of a specific component:
```
GET /nifi-api/processors/{id}/state
```
The response contains the scope and the stored values:
```json
{
  "componentState": {
    "componentId": "...",
    "state": {
      "scope": "CLUSTER",
      "state": {
        "last.timestamp": {
          "value": "..."
        }
      }
    }
  }
}
```

### Method 3. Via Files on Disk
For Local State, you can examine the contents of this directory:
```
$NIFI_HOME/state/local/
```
However, this is **internal storage**, not a convenient interface for reading state.

## 5. Recommendation

It is better to use **View state** or the **REST API**. These are the standard ways to view the processor's **logical state**, rather than the internal representation of the State Provider.

## Brief Recap
- State is stored in the State Management Provider, not in FlowFile attributes or repositories.
- Two types: Local (on the node, `$NIFI_HOME/state/local`) and Cluster (ZooKeeper by default; ConfigMaps are possible in Kubernetes).
- Configuration: `nifi.properties` + `state-management.xml`.
- The processor chooses the scope; state is isolated per component.
- Viewing: the UI (View state) and the REST API (`GET /nifi-api/processors/{id}/state`) are preferable to reading files on disk.

