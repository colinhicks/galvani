(ns galvani.client
  (:require [clojure.core.async :as async]
            [com.stuartsierra.dependency :as dependency]
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
  (let [^SequenceNumberRange snr (.getSequenceNumberRange shard)]
    {:stream-arn stream-arn
     :shard-id (.getShardId shard)
     :parent-shard-id (.getParentShardId shard)
     :starting-sequence-number (BigInteger. (.getStartingSequenceNumber snr))
     :ending-sequence-number (BigInteger. (.getEndingSequenceNumber snr))}))

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


(defn stop [{:keys [thread record-ch state-ch] :as stream-reader}]
  (run! async/close! (filter identity [thread record-ch state-ch]))
  (assoc stream-reader {:thread nil :record-ch nil :state-ch nil}))

(defn start
  [{:keys [client stream-arn record-ch state-ch record-batch-size parallelism keep-alive?
           timeout-ms iterator-type sequence-number]
    :as stream-reader}]
  (let [thread
        (async/thread
          (try
            (let [{:keys [shards] :as stream-info} (describe-stream client stream-arn)                 
                  graph (shard-graph shards)
                  pending-shard-ids (match-shards shards graph iterator-type sequence-number)
                  state {:pending-shard-ids pending-shard-ids
                         :sequence-number sequence-number}
                  iterate (fn [shard]
                            (if (= :update-marker shard)
                              {:update-marker true}
                              (let [iter (shard-iterator client stream-arn shard iterator-type sequence-number)]
                                {:iteration (read-shard client iter {:batch-size record-batch-size})})))
                  in-ch (async/chan 100)
                  pending-ch (async/chan 100)          
                  ex-handler (fn [ex] {:error ex})
                  close? (not keep-alive?)
                  cleanup (fn [] (run! async/close! [state-ch record-ch]))
                  _ (async/put! state-ch {:status :start :state state})
                  _ (async/onto-chan pending-ch pending-shard-ids close?)
                  _ (when keep-alive? (async/>!! pending-ch :update-marker))
                  _ (async/pipeline-blocking parallelism in-ch (map iterate) pending-ch close? ex-handler)]
              (loop [state state]
                (let [timeout-ch (async/timeout timeout-ms)
                      [{:keys [error iteration update-marker] :as val} port] (async/alts!! [in-ch timeout-ch])]
                  (cond
                    (= port timeout-ch)
                    (do
                      (async/>!! state-ch {:status :read-in :error :timeout :state state})
                      (if keep-alive?
                        (recur state)
                        (cleanup)))
                    
                    error
                    (do
                      (async/>!! state-ch {:status :read-in :error error :state state})
                      (if keep-alive?
                        (recur state)
                        (cleanup)))

                    (nil? val)
                    (do
                      (async/>!! state-ch {:status :exhausted :state state})
                      (cleanup))
                    
                    update-marker
                    (let [_ (async/>!! state-ch {:status :update-stream-description :state state})
                          next-stream-info (try
                                             (describe-stream client stream-arn)
                                             (catch Exception ex
                                               {:error ex}))]
                      (if-let [{:keys [error]} next-stream-info]
                        (do
                          (async/>!! state-ch {:status :update-stream-description :error error :state state})
                          (if keep-alive?
                            (recur state)
                            (cleanup)))
                        (let [next-shards (:shards next-stream-info)
                              next-pending-shard-ids (match-shards next-shards
                                                                   (shard-graph next-shards)
                                                                   iterator-type
                                                                   (:sequence-number state))
                              new-shard-ids (clojure.set/difference next-pending-shard-ids
                                                                    (:pending-shard-ids state))]
                          (if (seq new-shard-ids)
                            (async/<!! (async/onto-chan pending-ch new-shard-ids false))
                            ;; todo exponential backoff
                            (async/<!! (async/timeout timeout-ms)))
                          (async/>!! pending-ch :update-marker)
                          (recur (-> state
                                     (assoc :pending-shard-ids next-pending-shard-ids)
                                     (update :describe-stream-requests-since-iteration inc))))))

                    iteration
                    (do            
                      (async/<!! (async/onto-chan record-ch iteration false))
                      (recur (-> state
                                 (update :pending-shard-ids
                                         #(disj % (get-in (last iteration) [:iterator-info :shard-id])))
                                 (assoc :sequence-number
                                        (get-in (last iteration) [:dynamodb :sequence-number])
                                        :describe-stream-requests-since-iteration 0))))))))
            (catch Exception ex
              (async/put! state-ch {:status :before-start :error ex})
              nil)))]
    (assoc stream-reader :thread thread)))

(defrecord StreamReader [client stream-arn thread record-ch state-ch iterator-type sequence-number
                         parallelism record-batch-size timeout-ms keep-alive?])

(defn stream-reader [client stream-arn record-ch state-ch iterator-type
                     {:keys [record-batch-size parallelism keep-alive? timeout-ms sequence-number]
                      :or {record-batch-size 100
                           parallelism 4
                           keep-alive? false
                           timeout-ms 5000}
                      :as options}]
  (map->StreamReader (assoc options
                            :client client
                            :stream-arn stream-arn
                            :record-ch record-ch
                            :state-ch state-ch)))
