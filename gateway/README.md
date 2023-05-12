# Client

The clients library can be used either as a proxy or directly as a library if using Go.
The client has logic to communicate with the buffers and BigQuery to append, get and poll messages.
The client is stateless.

## Appending
The client communicates (watches) with etcd and stores the hash ranges in memory.
When a append request comes in the client will find the correct buffer to pass the append request along to.
The client will retry if it fails a configurable amount of times.

## etcd data structure

### Segment metadata
The persistent segment metadata used by 

Key:
/segments/{topic}/{time}_{hashFrom}-{hashTo}

Data:
JSON {
    HashFrom hash
    HashTo   hash
    TimeFrom nanotime
    TimeTo   nanotime
    Finished boolean
    Closed   boolean
}

### Hash range leases
Key:
/hash_range_leases/{topic}/{hashFrom}-{hashTo}

Data:
1

### Hash range sizes
Used to calculate size of each hash range should be for the topic. 
Total hashes: 0-65535
Size of each hash range given size: 2^factor hashes

Key: 
/topic_range_sizes/{topic}

Data:
int factor (results in hash range sizes of 2^factor)