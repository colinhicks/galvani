# Galvani

[![Clojars Project](https://img.shields.io/clojars/v/net.colinhicks/galvani.svg)](https://clojars.org/net.colinhicks/galvani)

## Usage

```clojure
(require '[colinhicks.galvani :as galvani])
(require '[clojure.core.async :as async])

;; 1: Put available records from the current snapshot of the shards onto a core.async channel ...
(defn dynamo-records-since [stream-arn checkpoint output-ch]
  (let [client (galvani/streams-client)]
    (galvani/start-reader
     (galvani/single-pass-reader client
                                 stream-arn
                                 checkpoint
                                 output-ch
                                 (fn [exception] (throw exception)))))
  output-ch)

;; 1A: Grab the oldest available record from the stream
(->> (async/chan 1)
     (dynamo-records-since "arn:aws:dynamodb..." :trim-horizon)
     (async/<!!))


;; 1B: Get new record images in sets of 10 after a sequence number
(->> (async/chan 100 (comp (keep (fn [record] (-> record :dynamodb :new-image)))
                           (partition-all 10)))
     (dynamo-records-since "arn:aws:dynamodb..." [:after-sequence-number 1234567890])
     ...)
     

;; By default, stream entities are parsed into clj records (see Record and StreamRecord in galvani.stream-parsing).
;; Where these records embed DynamoDB objects, a best-guess attempt is made at type inference, based on Faraday's approach.
;; In contrast to Faraday, byte fields are not assumed to be Nippy-serialized; it's up to your application to handle this case.
;; Override the reader's parser by passing an implementation of gavani.record-parsing's Parser protocol to the options object.
;; In turn, the parser must mint implementations of the RecordTracking protocol.
;; If you want to consume directly the AWS client's raw dynamodbv2.model.Record use the provided no-op-parser.

;; 2: Use the continuous reader to read the stream over an indefinite period

;; You may choose to use a lifecyle-management framework like stuartsierra.component
(extend-protocol component/Lifecycle
  galvani/StreamReader
  (start [component]
    (galvani/start-reader component))
  (stop [component]
    (galvani/stop-reader component)))

```


## Background: Consistent expectations

DynamoDB Streams capture time-ordered sequences of item-level modifications to DynamoDB Tables. Streams can simplify backup or cross-region replication. Or they can serve as the backbone for a log-based architecture. And their time-order guarantee is notable: It means ddb streams can help enforce consensus and at-most-once semantics in (carefully designed) distributed systems.

Given the implementation of any above example, ddb streams can be thought of as an architectural primitive, like other AWS products. Of course other solutions exist; this document makes no attempt at alternative comparison.

Pretty obvious, if you're reading this. But here's the take-away argument: **It's worth thinking of ddb streams on their own, not just as a feature of DynamoDB tables. That's why this code exists.**

### Don't let marketing's naming conflation paint your bikeshed.

With a different pricing model, DynamoDB Streams be a product separate from DynamoDB Tables, perhaps in the Kinesis family. Indeed ddb streams are compatible with the Kinesis Client Library, with caveats. You could probably say ddb streams offer a subset of Kinesis Streams functionality, with the added (out-of-box) guarantee that ddb streams are time-ordered, given their input source is constrained to DynamoDB Tables, supplying monotonically increasing sequence ids and deduplication. Anyway ...

> We speak ambiguous words, think ambiguous thoughts, and any project involving multiple people exists in a continuous state of low-level confusion.… A name whose sense is consistent with the reader’s expectations requires less effort from everyone. 
> — Zach Tellman, Elements of Clojure

Kinesis Streams, DynamoDB Streams, DynamoDB Tables. Amazon's conflating names invite confusion not only in dicussion; worse—they breed entanglement in our design & implementation. And if the names are here to stay, one way to mitigate ambiguity is by choosing narrower bounds for the scope of libraries interacting with these systems. Herein, an attempt.

### Acknowlegements
* Dynamo record type inference adapted from [taoensso/faraday](https://github.com/taoensso/faraday)
