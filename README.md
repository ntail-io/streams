# ntail streams
*ntail streams is currently in pre-alpha and is **not ready for production use***.

ntail streams is a fast message broker written in Go that uses Google BigQuery as a storage backend. 

## Roadmap
* Design improvements for scalability:
  * Improved coordination buffer nodes for picking up assigning hash ranges.
  * Improved coordination when subscribers pick up bookmarks.
* Access Control


## Installation
### Helm
```shell_script
helm repo add bitnami https://charts.ntail.io/ntail
helm install ntail-streams ntail/streams
```

## Why use ntail streams
### Simplicity
By storing messages directly in BigQuery, you have a shorter data pipeline and quicker insights.

### Cost savings
By utilizing cheap BigQuery storage, cost the is a fraction of other SaaS solutions. 

### Sub-millisecond response times
ntails streams stores the tail of each topic in memory to serve messages blazingly fast.

### Faster insights
Messages are accessible on BigQuery **atomically**. This means you have access to **real-time analytics**, not near time analytics.

### No partitions
Ntail streams has no fixed partitions, instead messages are grouped by segments dynamically (this feature is not yet implemented).

### Ordering guarantee
ntail streams messages are ordered sequentially by key. This makes it excellent to use for communication in a microservice architecture.

### Infinite retention without compromises
Unlike other message brokers there is no limit to the amount of data that can be stored.


