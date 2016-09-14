# Faraday streams

## A Clojure client for reading DynamoDB streams


### Usage

```clojure
(require '[faraday-streams.client :as fsc])

(defn all-available-records [stream-arn]
  (let [client (fsc/streams-client)
        oldest-shard-id (-> client
                            (fsc/describe-stream stream-arn)
                            (fsc/oldest-shard-id))
        shard-iterator (fsc/shard-iterator client stream-arn oldest-shard-id :trim-horizon)]
    (fsc/records client shard-iterator)))

(->> (all-available-records "arn:aws:dynamodb...")
     (filter #(-> % :dynamodb :new-item :something-interesting?))
     (take 5))
```


### Background: Consistent expectations

DynamoDB Streams capture time-ordered sequences of item-level modifications to DynamoDB Tables. Streams can simplify backup or cross-region replication. Or they can serve as the backbone for a log-based architecture. And their time-order guarantee is notable: It means ddb streams can help enforce consensus and at-most-once semantics in (carefully designed) distributed systems.

Given the implementation of any above example, ddb streams can be thought of as an architectural primitive, like other AWS products. Of course other solutions exist; this document makes no attempt at alternative comparison.

Pretty obvious, if you're reading this. But here's the take-away argument: **It's worth thinking of ddb streams on their own, not just as a feature of DynamoDB tables. That's why this code exists.**

> ### Don't let marketing's naming conflation paint your bikeshed.

With a different pricing model, DynamoDB Streams be a product separate from DynamoDB Tables, perhaps in the Kinesis family. Indeed ddb streams are compatible with the Kinesis Client Library, with caveats. You could probably say ddb streams offer a subset of Kinesis Streams functionality, with the added (out-of-box) guarantee that ddb streams are time-ordered, given their input source is constrained to DynamoDB Tables, supplying monotonically increasing sequence ids and deduplication. Anyway ...

> We speak ambiguous words, think ambiguous thoughts, and any project involving multiple people exists in a continuous state of low-level confusion.… A name whose sense is consistent with the reader’s expectations requires less effort from everyone. 
> — Zach Tellman, Elements of Clojure

Kinesis Streams, DynamoDB Streams, DynamoDB Tables. Amazon's conflating names invite confusion not only in dicussion; worse—they breed entanglement in our design & implementation. And if the names are here to stay, one way to mitigate ambiguity is by choosing narrower bounds for the scope of libraries interacting with these systems. Herein, an attempt.
