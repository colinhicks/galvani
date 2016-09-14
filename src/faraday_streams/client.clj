(ns faraday-streams.client
  (:require [taoensso.faraday :as faraday]
            [taoensso.faraday.utils :as faraday-utils])
  (:import [com.amazonaws.auth EnvironmentVariableCredentialsProvider]
           [com.amazonaws.services.dynamodbv2 AmazonDynamoDBStreamsClient AmazonDynamoDBStreams]
           [com.amazonaws.services.dynamodbv2.model AttributeValue DescribeStreamRequest GetRecordsRequest
            GetRecordsResult GetShardIteratorRequest GetShardIteratorResult Record Shard ShardIteratorType
            StreamDescription StreamRecord]))


(defn db-val->clj-val [val]
  (if-not (map? val)
    (#'faraday/db-val->clj-val ^AttributeValue val)
    (let [[type x] (first val)
          type' (-> type name clojure.string/lower-case keyword)]
      (case type'
        :s    x
        :n    (#'faraday/ddb-num-str->num x)
        :null nil
        :bool (boolean  x)
        :ss   (into #{} x)
        :ns   (into #{} (mapv #'faraday/ddb-num-str->num x))
        :bs   (into #{} (mapv #'faraday/nt-thaw x))
        :b    (#'faraday/nt-thaw  x)
        :l    (mapv db-val->clj-val x)
        :m    x))))

(defn ddb-item->clj-item [item]
  (faraday-utils/keyword-map db-val->clj-val item))

(defn streams-client
  ([]
   (AmazonDynamoDBStreamsClient. (EnvironmentVariableCredentialsProvider.)))
  ([endpoint]
   (doto (streams-client)
     (.setEndpoint endpoint))))

(defn describe-stream [^AmazonDynamoDBStreams client arn]
  (let [^DescribeStreamRequest req (.withStreamArn (DescribeStreamRequest.) arn)
        ^StreamDescription description (.getStreamDescription (.describeStream client req))]
    {:stream-arn (.getStreamArn description)
     :stream-label (.getStreamLabel description)
     :stream-status (.getStreamStatus description)
     :stream-view-type (.getStreamViewType description)
     :creation-request-date-time (.getCreationRequestDateTime description)
     :table-name (.getTableName description)
     :key-schema (.getKeySchema description)
     :shards (.getShards description)
     :last-evaluated-shard-id (.getLastEvaluatedShardId description)}))

(def shard-iterator-types
  {:trim-horizon ShardIteratorType/TRIM_HORIZON
   :latest ShardIteratorType/LATEST
   :after-sequence-number ShardIteratorType/AFTER_SEQUENCE_NUMBER
   :at-sequence-number ShardIteratorType/AT_SEQUENCE_NUMBER})

(defn shard-iterator [^AmazonDynamoDBStreams client arn shard-id shard-iterator-kw & [sequence-number]]
  (let [type (get shard-iterator-types shard-iterator-kw)
        _ (assert type (str "Must be one of " (keys shard-iterator-types)))
        _ (when (#{:after-sequence-number :at-sequence-number} shard-iterator-kw)
            (assert sequence-number "Required with iterator type :after-sequence-number or :at-sequence-number"))
        ^GetShardIteratorRequest req (-> (GetShardIteratorRequest.)
                                         (.withStreamArn arn)
                                         (.withShardId shard-id)
                                         (.withShardIteratorType type)
                                         (.withSequenceNumber sequence-number))
        ^GetShardIteratorResult res (.getShardIterator client req)]
    (.getShardIterator res)))

(defn records-batch [^AmazonDynamoDBStreams client shard-iterator]
  (let [^GetRecordsRequest req (-> (GetRecordsRequest.)
                                   (.withShardIterator shard-iterator))
        ^GetRecordsResult res (.getRecords client req)
        batch (.getRecords res)]
    {:records batch
     :next-shard-iterator (.getNextShardIterator res)}))

(defn stream-record->clj [^StreamRecord stream-record]
  {:keys (ddb-item->clj-item (.getKeys stream-record))
   :new-image (ddb-item->clj-item (.getNewImage stream-record))
   :old-image (ddb-item->clj-item (.getOldImage stream-record))
   :sequence-number (.getSequenceNumber stream-record)
   :size-bytes (.getSizeBytes stream-record)
   :stream-view-type (.getStreamViewType stream-record)})

(defn record->clj [^Record record]
  {:event-id (.getEventID record)
   :event-name (.getEventName record)
   :event-version (.getEventVersion record)
   :event-source (.getEventSource record)
   :aws-region (.getAwsRegion record)
   :dynamodb (stream-record->clj (.getDynamodb record))})

(defn records
  ([^AmazonDynamoDBStreams client shard-iterator]
   (records client shard-iterator []))
  ([^AmazonDynamoDBStreams client shard-iterator xs]
   (lazy-seq
    (if (seq xs)
      (cons (record->clj (first xs)) (records client shard-iterator (rest xs)))
      (if shard-iterator
        (let [batch (records-batch client shard-iterator)]
          (records client (:next-shard-iterator batch) (:records batch))))))))
