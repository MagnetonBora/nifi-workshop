---
title: "Splunk Delivery: TCP vs HEC"
---
## Introduction

In [[labs/lab-7|Lab 7]] the same events are delivered to Splunk in two ways: over raw TCP with `PutSplunk` and over the **HTTP Event Collector (HEC)** with `InvokeHTTP`. A natural question after the lab is which one to use in practice.

**Short answer:** for pure throughput TCP is slightly cheaper. Overall, choose **HEC** (`InvokeHTTP` or `PutSplunkHTTP`) with batching through `MergeRecord`.

This summary compares the two transports, explains why the lab does not show a speed difference, and gives a recommendation for production.

## 1. PutSplunk (TCP)

- **Less overhead:** no HTTP headers and no authentication, so each event costs slightly less CPU and network traffic.
- **No delivery guarantee:** `success` in NiFi only means the bytes were written to the socket, not that Splunk indexed them. If Splunk restarts or the connection drops, part of the data is lost **silently**.
- **Fixed index and sourcetype:** they are set on the Splunk side, in the input settings (`[tcp://5514]`). NiFi cannot change them.
- **No authentication or TLS out of the box;** such an input is also awkward to put behind a load balancer.

## 2. HEC (InvokeHTTP)

- **Every request gets an HTTP response code.** Errors are visible immediately and can be handled: retry, a `failure` branch. For example, a `400` response points to a malformed request or payload.
- **Guaranteed delivery is possible:** enable indexer acknowledgement (`useACK`) on the token.
- **Token and TLS;** index and sourcetype can be passed in every request; load balancing works as with any HTTP service.
- **HTTP overhead is negligible with batches:** with about 200 KB per request (10,000 events), the bottleneck is indexing in Splunk, not the transport.

## 3. Comparison

| | `PutSplunk` (TCP) | HEC (`InvokeHTTP` / `PutSplunkHTTP`) |
|---|---|---|
| **Overhead per event** | Minimal | Slightly higher; negligible with batches |
| **What `success` means** | Bytes went into the socket | Splunk accepted the request (with ACK: indexed) |
| **Error visibility** | None; losses are silent | HTTP code on every request |
| **Index / sourcetype** | Fixed in the Splunk input | Can be set per request |
| **Authentication / TLS** | Not out of the box | Token + TLS |
| **Load balancer** | Awkward | Standard HTTP balancing |

## 4. Why the Lab Shows No Speed Difference

In the lab both branches are limited by the **generator** (`GenerateRecord`), not by the delivery method. The transport never becomes the bottleneck, so the measured throughput is the same.

## 5. Recommendation

- **Production:** HEC with large batches. Splunk itself recommends HEC over raw TCP.
- NiFi has a dedicated processor, **`PutSplunkHTTP`**. Together with **`QuerySplunkIndexingStatus`** it checks through ACK that an event was actually indexed. It also removes the need to build the `Authorization` header by hand in `InvokeHTTP`.
- **TCP** only fits when losing some events is acceptable (for example, debug logs) and every bit of CPU matters.

## Brief Recap
- TCP (`PutSplunk`) is slightly cheaper but gives no delivery guarantee: `success` means "written to the socket".
- HEC returns an HTTP code per request, supports ACK, tokens, TLS, and per-request index/sourcetype.
- With batches of ~10,000 events the HTTP overhead is negligible; Splunk indexing becomes the limit.
- In Lab 7 both variants are limited by the generator, so the speed is the same.
- Production choice: HEC with `MergeRecord` batches, ideally `PutSplunkHTTP` + `QuerySplunkIndexingStatus`.
