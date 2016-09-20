(ns galvani.client
  (:require [clojure.core.async :as async]
            [galvani.record-parsing :as record-parsing])
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

(defn shard->clj [stream-arn ^Shard shard]
  {:stream-arn stream-arn
   :shard-id (.getShardId shard)
   :parent-shard-id (.getParentShardId shard)
   :sequence-number-range
   (let [^SequenceNumberRange snr (.getSequenceNumberRange shard)]
     {:starting-sequence-number (.getStartingSequenceNumber snr)
      :ending-sequence-number (.getEndingSequenceNumber snr)})})

(defn describe-stream [^AmazonDynamoDBStreams client arn]
  (let [^DescribeStreamRequest req (.withStreamArn (DescribeStreamRequest.) arn)
        ^StreamDescription description (.getStreamDescription ^DescribeStreamResult (.describeStream client req))
        stream-arn (.getStreamArn description)]
    {:stream-arn stream-arn
     :stream-label (.getStreamLabel description)
     :stream-status (.getStreamStatus description)
     :stream-view-type (.getStreamViewType description)
     :creation-request-date-time (.getCreationRequestDateTime description)
     :table-name (.getTableName description)
     :key-schema (.getKeySchema description)
     :shards (mapv (partial shard->clj stream-arn) (.getShards description))
     :last-evaluated-shard-id (.getLastEvaluatedShardId description)
     :timestamp (System/currentTimeMillis)}))

(def shard-iterator-types
  {:trim-horizon ShardIteratorType/TRIM_HORIZON
   :latest ShardIteratorType/LATEST
   :after-sequence-number ShardIteratorType/AFTER_SEQUENCE_NUMBER
   :at-sequence-number ShardIteratorType/AT_SEQUENCE_NUMBER})

(defn shard-iterator [^AmazonDynamoDBStreams client arn shard-id iterator-type & [sequence-number]]
  (let [type (get shard-iterator-types iterator-type)
        _ (assert type (str "Must be one of " (keys shard-iterator-types)))
        _ (when (#{:after-sequence-number :at-sequence-number} iterator-type)
            (assert sequence-number "Required with iterator type :after-sequence-number or :at-sequence-number"))
        ^GetShardIteratorRequest req (-> (GetShardIteratorRequest.)
                                         (.withStreamArn arn)
                                         (.withShardId shard-id)
                                         (.withShardIteratorType type)
                                         (.withSequenceNumber sequence-number))
        ^GetShardIteratorResult res (.getShardIterator client req)]
    {:iterator (.getShardIterator res)
     :type iterator-type
     :timestamp (System/currentTimeMillis)
     :initial-sequence-number sequence-number
     :shard-id shard-id
     :stream-arn arn}))

(defn records-batch [^AmazonDynamoDBStreams client shard-iterator batch-size]
  (let [^GetRecordsRequest req (-> (GetRecordsRequest.)
                                   (.withShardIterator shard-iterator)
                                   (.withLimit (int batch-size)))
        ^GetRecordsResult res (.getRecords client req)
        batch (.getRecords res)]
    {:records batch
     :next-shard-iterator (.getNextShardIterator res)}))

(defn read-shard* [client {:keys [iterator] :as iterator-info} aconv n xs]
  (lazy-seq
   (if (seq xs)
     (cons (record-parsing/record->clj (first xs) iterator-info aconv)
           (read-shard* client iterator-info aconv n (rest xs)))
     (if iterator
       (let [{:keys [records next-shard-iterator]} (records-batch client iterator n)
             next-iterator-info (assoc iterator-info :iterator next-shard-iterator)]
         (read-shard* client next-iterator-info aconv n records))))))

(defn read-shard
  ([client iterator-info]
   (read-shard client iterator-info {}))
  ([client iterator-info {:keys [batch-size attribute-converter]
                          :or {batch-size 100
                               attribute-converter record-parsing/default-attribute-converter}}]
   (read-shard* client iterator-info attribute-converter batch-size [])))


(defn read-shards [client iterator-infos record-opts]
  (let [out-ch (async/chan (count iterator-infos))
        iter-chs (map #(async/thread (read-shard client % record-opts)) iterator-infos)]
    (async/go-loop [chs iter-chs]
      (let [[iter-result port] (async/alts! chs)
            next-chs (filterv #(not= port %) chs)]
        (when (seq iter-result)
          (loop [[x & xs] iter-result]
            (when x
              (async/>! out-ch x)
              (recur xs)))
          (async/close! port))
        (if (seq next-chs)
          (recur next-chs)
          (async/close! out-ch))))
    out-ch))

(defn match-shards [stream-info iterator-type & [sequence-number]])

(defn start [stream-reader stream-info iterator-type & [sequence-number]])

(defn update [stream-reader next-stream-info])

(defn tap [stream-reader ch]
  (async/tap (:out-mult stream-reader) ch))

(defn only-record [message]
  (:record message))

(defrecord StreamReader [client stream-info state in-mix in-ch out-ch out-mult])

(defn stream-reader [client record-batch-size out-buffer]
  (let [in-ch (async/chan 10)
        in-mix (async/mix in-ch)
        out-ch (async/chan (keep only-record) out-buffer)
        out-mult (async/mult out-ch)
        _ (async/pipe in-ch out-ch)]
    (map->StreamReader {:client client
                        :in-ch in-ch
                        :in-mix in-mix
                        :out-ch out-ch
                        :out-mult out-mult})))
