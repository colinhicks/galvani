(ns galvani.client
  (:require [galvani.record-parsing :as record-parsing])
  (:import [com.amazonaws.auth EnvironmentVariableCredentialsProvider]
           [com.amazonaws.services.dynamodbv2 AmazonDynamoDBStreamsClient AmazonDynamoDBStreams]
           [com.amazonaws.services.dynamodbv2.model DescribeStreamRequest DescribeStreamResult
            GetRecordsRequest GetRecordsResult GetShardIteratorRequest GetShardIteratorResult Shard
            ShardIteratorType StreamDescription SequenceNumberRange]))


(defn streams-client
  ([]
   (AmazonDynamoDBStreamsClient. (EnvironmentVariableCredentialsProvider.)))
  ([endpoint]
   (doto (streams-client)
     (.setEndpoint endpoint))))

(defn shard->clj [^Shard shard]
  {:shard-id (.getShardId shard)
   :parent-shard-id (.getParentShardId shard)
   :sequence-number-range
   (let [^SequenceNumberRange snr (.getSequenceNumberRange shard)]
     {:starting-sequence-number (.getStartingSequenceNumber snr)
      :ending-sequence-number (.getEndingSequenceNumber snr)})})

(defn describe-stream [^AmazonDynamoDBStreams client arn]
  (let [^DescribeStreamRequest req (.withStreamArn (DescribeStreamRequest.) arn)
        ^StreamDescription description (.getStreamDescription ^DescribeStreamResult (.describeStream client req))]
    {:stream-arn (.getStreamArn description)
     :stream-label (.getStreamLabel description)
     :stream-status (.getStreamStatus description)
     :stream-view-type (.getStreamViewType description)
     :creation-request-date-time (.getCreationRequestDateTime description)
     :table-name (.getTableName description)
     :key-schema (.getKeySchema description)
     :shards (mapv shard->clj (.getShards description))
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

(defn records*
  [^AmazonDynamoDBStreams client iter aconv xs]
  (lazy-seq
   (if (seq xs)
     (cons (record-parsing/record->clj (first xs) aconv)
           (records* client iter aconv (rest xs)))
     (if iter
       (let [batch (records-batch client iter)]
         (records* client
                   (:next-shard-iterator batch)
                   aconv
                   (:records batch)))))))

(defn records
  ([client shard-iterator]
   (records* client shard-iterator record-parsing/default-attribute-converter []))
  ([client shard-iterator attribute-converter]
   (records* client shard-iterator attribute-converter [])))


(defn walk-shards
  "walk shards, returning records in shard-lineage order, based on iterator kw.
   in trim horizon case, use first shard and walk forward in time
   in latest case, use last shard and walk backward in time
   sequence number cases use last two arguments: sequence-number and either :trim-horizon or :latest, representing destination
   in sequence number cases, find an appropriate shard and walk corresponding direction"
  [client stream-description shard-iterator-kw & [sequence-number directional-shard-iterator-kw]])
