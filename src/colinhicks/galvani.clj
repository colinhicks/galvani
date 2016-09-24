(ns colinhicks.galvani
  (:require [clojure.core.async :as async]
            [com.stuartsierra.dependency :as dependency]
            [colinhicks.galvani.record-parsing :as record-parsing])
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
  (let [^SequenceNumberRange snr (.getSequenceNumberRange shard)
        ending (.getEndingSequenceNumber snr)]
    {:stream-arn stream-arn
     :shard-id (.getShardId shard)
     :parent-shard-id (.getParentShardId shard)
     :starting-sequence-number (BigInteger. (.getStartingSequenceNumber snr))
     :ending-sequence-number (when-not (clojure.string/blank? ending)
                               (BigInteger. ending))}))

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
                                         (.withSequenceNumber (str sequence-number)))
        ^GetShardIteratorResult res (.getShardIterator client req)]
    {:iterator (.getShardIterator res)
     :type iterator-type
     :timestamp (System/currentTimeMillis)
     :initial-sequence-number (str sequence-number)
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

(defn read-shard* [client {:keys [iterator] :as iterator-info} parser n xs]
  (lazy-seq
   (if (seq xs)
     (cons (record-parsing/parse-record parser (first xs))
           (read-shard* client iterator-info parser n (rest xs)))
     (if iterator
       (let [{:keys [records next-shard-iterator]} (records-batch client iterator n)
             next-iterator-info (assoc iterator-info :iterator next-shard-iterator)]
         (read-shard* client next-iterator-info parser n records))))))

(defn read-shard [client iterator-info record-parser record-batch-size]
  (read-shard* client iterator-info record-parser record-batch-size []))

(defn shard-graph [shards]
  (let [available-shard-ids (set (map :shard-id shards))]
    (reduce (fn [g {:keys [shard-id parent-shard-id ending-sequence-number]}]
              (let [available? (available-shard-ids parent-shard-id)]
                (cond-> g
                  available?
                  (dependency/depend shard-id parent-shard-id)

                  (not available?)
                  (dependency/depend shard-id :trim-horizon)

                  (nil? ending-sequence-number)
                  (dependency/depend :latest shard-id))))
            (dependency/graph)
            shards)))

(defn match-shards [shards graph iterator-type & [sequence-number]]
  (let [shard-ids (case iterator-type
                    :trim-horizon (dependency/transitive-dependents graph :trim-horizon)
                    :latest (dependency/immediate-dependencies graph :latest)
                    (:at-sequence-number :after-sequence-number)
                    (if-let [incl (some (fn [{:keys [starting-sequence-number ending-sequence-number shard-id]}]
                                          (when (<= starting-sequence-number
                                                    sequence-number
                                                    ending-sequence-number)
                                            shard-id))
                                        shards)]
                      (conj (dependency/transitive-dependents graph incl) incl)
                      #{}))
        shard-ids' (clojure.set/difference shard-ids #{:trim-horizon :latest})]
    (apply sorted-set-by (dependency/topo-comparator graph) shard-ids')))


(defn stop-reader [{:keys [poison-ch] :as stream-reader}]
  (async/put! poison-ch :shutdown)
  (assoc stream-reader
         :thread nil
         :record-ch nil
         :state-ch nil))

(defn start-reader
  [{:keys [client stream-arn record-ch state-ch poison-ch record-parser record-batch-size
           keep-alive? timeout-ms iterator-type sequence-number]
    :as stream-reader}]
  (let [thread
        (async/thread
          (try
            (let [{:keys [shards] :as stream-info} (describe-stream client stream-arn)                 
                  graph (shard-graph shards)
                  pending-shard-ids (match-shards shards graph iterator-type sequence-number)
                  initial-state {:pending-shard-ids pending-shard-ids
                                 :processed-shard-ids #{}
                                 :sequence-number sequence-number
                                 :describe-stream-requests-since-iteration 0}
                  iterate (fn [shard]
                            (if (= :update-marker shard)
                              {:update-marker true}
                              (let [iter (shard-iterator client stream-arn shard iterator-type sequence-number)]
                                {:iteration (read-shard client iter record-parser record-batch-size)
                                 :shard-id shard})))
                  in-ch (async/chan 100)
                  pending-ch (async/chan 100)          
                  ex-handler (fn [ex] {:error ex})
                  close? (not keep-alive?)
                  close-chs (fn [] (run! async/close! [state-ch record-ch in-ch]))
                  _ (async/put! state-ch {:status :start :state initial-state})
                  _ (async/onto-chan pending-ch pending-shard-ids close?)
                  _ (when keep-alive? (async/>!! pending-ch :update-marker))
                  _ (async/pipeline-blocking 1 in-ch (map iterate) pending-ch close? ex-handler)]
              (loop [state initial-state]
                (let [timeout-ch (async/timeout timeout-ms)
                      [{:keys [error iteration update-marker] :as val} port]
                      (async/alts!! [poison-ch in-ch timeout-ch] :priority true)]
                  (cond
                    (= port timeout-ch)
                    (do
                      (async/>!! state-ch {:status :read-in :error :timeout :state state})
                      (recur state))

                    (= port poison-ch)
                    (do
                      (async/>!! state-ch {:status :shutting-down :state state})
                      (close-chs))
                    
                    error
                    (do
                      (async/>!! state-ch {:status :read-in :error error :state state})
                      (recur state))

                    (nil? val)
                    (do
                      (async/>!! state-ch {:status :exhausted :state state})
                      (close-chs))
                    
                    update-marker
                    (let [_ (async/>!! state-ch {:status :update-stream-description :state state})
                          next-stream-info (try
                                             (describe-stream client stream-arn)
                                             (catch Exception ex
                                               {:error ex}))]
                      (if-let [error (:error next-stream-info)]
                        (do
                          (async/>!! state-ch {:status :update-stream-description :error error :state state})
                          (recur state))
                        (let [next-shards (:shards next-stream-info)
                              next-pending-shard-ids (match-shards next-shards
                                                                   (shard-graph next-shards)
                                                                   iterator-type
                                                                   (:sequence-number state))
                              new-shard-ids (clojure.set/difference next-pending-shard-ids
                                                                    (:pending-shard-ids state)
                                                                    (:processed-shard-ids state))]
                          (if (seq new-shard-ids)
                            (async/<!! (async/onto-chan pending-ch new-shard-ids false))
                            ;; todo: exponential backoff
                            (async/<!! (async/timeout timeout-ms)))
                          (async/>!! pending-ch :update-marker)
                          (recur (-> state
                                     (assoc :pending-shard-ids next-pending-shard-ids)
                                     (update :describe-stream-requests-since-iteration inc))))))

                    iteration
                    (let [shard-id (:shard-id val)
                          state (-> state
                                 (update :pending-shard-ids
                                         #(disj % shard-id))
                                 (update :processed-shard-ids
                                         #(conj % shard-id)))]
                      (if (seq iteration)
                        (do            
                          (async/<!! (async/onto-chan record-ch iteration false))
                          (recur (assoc state
                                        :sequence-number (record-parsing/sequence-number (last iteration))
                                        :describe-stream-requests-since-iteration 0)))
                        (recur state)))))))
            (catch Exception ex
              (async/>!! state-ch {:status :before-start :error ex})
              nil)))]
    (assoc stream-reader :thread thread)))

(defrecord StreamReader [client stream-arn thread record-ch state-ch poison-ch iterator-type sequence-number
                         record-parser record-batch-size timeout-ms keep-alive?])

(defn normalize-checkpoint [checkpoint]
  (if-not (vector? checkpoint)
    (vector checkpoint)
    checkpoint))

(def default-parser (record-parsing/default-parser))
(def no-op-parser (record-parsing/no-op-parser))

(defn continuous-reader [client stream-arn checkpoint record-ch state-ch & [opts]]
  (let [[iterator-type & [sequence-number]] (normalize-checkpoint checkpoint)
        {:keys [record-parser record-batch-size timeout-ms keep-alive?]
         :or {record-parser default-parser
              record-batch-size 100
              timeout-ms 5000
              keep-alive? true}}
        opts]
    (map->StreamReader {:client client
                        :stream-arn stream-arn
                        :iterator-type iterator-type
                        :record-ch record-ch
                        :state-ch state-ch
                        :poison-ch (async/chan 1)
                        :sequence-number sequence-number
                        :record-parser record-parser
                        :record-batch-size record-batch-size
                        :keep-alive? keep-alive?
                        :timeout-ms timeout-ms})))

(defn single-pass-reader [client stream-arn checkpoint record-ch ex-handler & [opts]]
  (let [state-ch (async/chan (async/sliding-buffer 1))
        reader (continuous-reader client stream-arn checkpoint record-ch
                                  state-ch (assoc opts :keep-alive? false))]
    (async/go-loop []
      (let [update (async/<! state-ch)]
        (cond
          (nil? update)
          (stop-reader reader)

          (:error update)
          (do
            (stop-reader reader)
            (ex-handler (:error update)))

          :default
          (recur))))
    reader))
