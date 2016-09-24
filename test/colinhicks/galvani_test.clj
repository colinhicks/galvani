(ns colinhicks.galvani-test
  (:require [colinhicks.galvani :as galvani]
            [colinhicks.galvani.record-parsing :as record-parsing]
            [com.stuartsierra.dependency :as dependency]
            [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as async])
  (:import [com.amazonaws.services.dynamodbv2 AmazonDynamoDBStreams]
           [com.amazonaws.services.dynamodbv2.model Record StreamRecord AttributeValue
            GetRecordsResult GetShardIteratorResult Shard SequenceNumberRange DescribeStreamResult StreamDescription]))


(deftest read-shard
  (let [iters (atom ["1000" "2000" "3000"])
        mock-record (fn [shard name]
                      (let [id-val (-> (AttributeValue.) (.withN (str shard name)))
                            name-val (-> (AttributeValue.) (.withS name))]
                        (-> (Record.)
                            (.withDynamodb
                             (-> (StreamRecord.)
                                 (.withKeys (java.util.HashMap. {"id" id-val}))
                                 (.withNewImage (java.util.HashMap. {"id" id-val
                                                                     "name" name-val})))))))
        mock-client (reify AmazonDynamoDBStreams
                      (getRecords [client req]
                        (proxy [GetRecordsResult] []
                          (getRecords []
                            (mapv (partial mock-record (first @iters)) ["100" "101" "102"]))
                          (getNextShardIterator []
                            (first (swap! iters rest))))))
        recs (galvani/read-shard mock-client
                                {:iterator (first @iters)}
                                (record-parsing/default-parser)
                                100)]
    (is (instance? clojure.lang.LazySeq recs))
    (is (= {:name "100" :id 1000100N} (->> recs (take 4) first :dynamodb :new-image)))
    (is (= "2000" (first @iters)))
    (last (take 4 recs))
    (is (= "3000" (first @iters)))
    (is (= {:name "102" :id 3000102N} (->> recs last :dynamodb :new-image)))
    (is (= 9 (count (take 999 recs))))))


(deftest read-shard-empty-iterator
  (let [iters (atom [10 0 9 1])
        expected-count (reduce + @iters)
        mock-client (reify AmazonDynamoDBStreams
                      (getRecords [client req]
                        (proxy [GetRecordsResult] []
                          (getRecords []
                            (repeat (first @iters)
                                    (-> (Record.) (.withDynamodb (StreamRecord.)))))
                          (getNextShardIterator []
                            (when-let [iter (first (swap! iters rest))]
                              (str iter))))))
        recs (galvani/read-shard mock-client
                                {:iterator (str (first @iters))}
                                (record-parsing/default-parser)
                                100)]
    (is (= expected-count (count recs)))))


(deftest describe-stream
  (let [shard (fn [[id parent-id start end]] (-> (Shard.)
                                                 (.withShardId id)
                                                 (.withParentShardId parent-id)
                                                 (.withSequenceNumberRange
                                                  (-> (SequenceNumberRange.)
                                                      (.withStartingSequenceNumber start)
                                                      (.withEndingSequenceNumber end)))))
        shard-params [["shardId-1-p3as6f" nil "10000001" "10000002"]
                      ["shardId-2-3n1xaf" "shardId-1-p3as6f" "10000005" "10000007"]
                      ["shardId-3-v13n3d" "shardId-2-3n1xaf" "10000008" "10000009"]]
        shards (mapv shard shard-params)
        mock-client (reify AmazonDynamoDBStreams
                      (describeStream [client req]
                        (-> (DescribeStreamResult.)
                            (.withStreamDescription
                             (-> (StreamDescription.)
                                 (.withShards shards))))))
        stream-description (galvani/describe-stream mock-client nil)]
    (is (= (mapv first shard-params) (mapv :shard-id (:shards stream-description))))
    (is (= (take 2 (mapv :shard-id (:shards stream-description)))
           (drop 1 (mapv :parent-shard-id (:shards stream-description)))))
    (galvani/describe-stream mock-client nil)))


(deftest match-shards
  (let [shards [{:shard-id "shardId-00000001473809200686-70269655" 
                 :parent-shard-id "shardId-00000001473796542190-2bd0df1e" 
                 :ending-sequence-number 168986800000000010966096477N
                 :starting-sequence-number 168986800000000010966096476N} 
                {:shard-id "shardId-00000001473822397022-2f1e8861" 
                 :parent-shard-id "shardId-00000001473809200686-70269655" 
                 :ending-sequence-number 169713500000000013292899685N 
                 :starting-sequence-number 169713500000000013292899684N} 
                {:shard-id "shardId-00000001473836343761-d0e55bda" 
                 :parent-shard-id "shardId-00000001473822397022-2f1e8861" 
                 :ending-sequence-number 170481300000000008600933632N 
                 :starting-sequence-number 170481300000000008600933631N} 
                {:shard-id "shardId-00000001473850173885-a8daa037" 
                 :parent-shard-id "shardId-00000001473836343761-d0e55bda" 
                 :ending-sequence-number 170481300000000008600993638N 
                 :starting-sequence-number 170481300000000008600993637N} 
                {:shard-id "shardId-00000001473863878679-3c26c6b9" 
                 :parent-shard-id "shardId-00000001473850173885-a8daa037" 
                 :ending-sequence-number 171997500000000008527703142N 
                 :starting-sequence-number 171997500000000008527703141N} 
                {:shard-id "shardId-00000001473879823062-fb383dfe" 
                 :parent-shard-id "shardId-00000001473863878679-3c26c6b9" 
                 :ending-sequence-number 172875200000000012689616965N 
                 :starting-sequence-number 172875200000000012689616964N} 
                {:shard-id "shardId-00000001473895717417-f02c79a0" 
                 :parent-shard-id "shardId-00000001473879823062-fb383dfe" 
                 :starting-sequence-number 173750800000000011849530822N}]
        graph (galvani/shard-graph shards)]
    (is (= #{"shardId-00000001473809200686-70269655" :trim-horizon}
           (dependency/transitive-dependencies graph (:shard-id (second shards)))))
    (is (= #{"shardId-00000001473895717417-f02c79a0"}
           (dependency/immediate-dependencies graph :latest)))

    (is (= ["shardId-00000001473836343761-d0e55bda"
            "shardId-00000001473850173885-a8daa037"
            "shardId-00000001473863878679-3c26c6b9"
            "shardId-00000001473879823062-fb383dfe"
            "shardId-00000001473895717417-f02c79a0"]
           (vec (galvani/match-shards shards graph :at-sequence-number 170481300000000008600933632N))))
    
    (is (empty?
         (galvani/match-shards shards graph :at-sequence-number 0)))

    (is (= #{"shardId-00000001473895717417-f02c79a0"}
           (galvani/match-shards shards graph :latest)))

    (is (= (->> shards (sort-by :starting-sequence-number) (map :shard-id))
           (vec (galvani/match-shards shards graph :trim-horizon))))))

(deftest stream-reader
  (let [describe-stream-invocations (atom 0)
        shards [{:shard-id "shardId-00000001473809200686-70269655" 
                 :parent-shard-id "shardId-00000001473796542190-2bd0df1e" 
                 :ending-sequence-number 168986800000000010966096477N
                 :starting-sequence-number 168986800000000010966096476N} 
                {:shard-id "shardId-00000001473822397022-2f1e8861" 
                 :parent-shard-id "shardId-00000001473809200686-70269655" 
                 :ending-sequence-number 169713500000000013292899685N 
                 :starting-sequence-number 169713500000000013292899684N} 
                {:shard-id "shardId-00000001473836343761-d0e55bda" 
                 :parent-shard-id "shardId-00000001473822397022-2f1e8861" 
                 :ending-sequence-number 170481300000000008600933632N 
                 :starting-sequence-number 170481300000000008600933631N} 
                {:shard-id "shardId-00000001473850173885-a8daa037" 
                 :parent-shard-id "shardId-00000001473836343761-d0e55bda" 
                 :ending-sequence-number 170481300000000008600933938N 
                 :starting-sequence-number 170481300000000008600993637N} 
                {:shard-id "shardId-00000001473863878679-3c26c6b9" 
                 :parent-shard-id "shardId-00000001473850173885-a8daa037" 
                 :ending-sequence-number 170481300000000008600993638N
                 :starting-sequence-number 170481300000000008600993637N} 
                {:shard-id "shardId-00000001473879823062-fb383dfe" 
                 :parent-shard-id "shardId-00000001473863878679-3c26c6b9" 
                 :ending-sequence-number 172875200000000012689616965N 
                 :starting-sequence-number 172875200000000012689616964N} 
                {:shard-id "shardId-00000001473895717417-f02c79a0" 
                 :parent-shard-id "shardId-00000001473879823062-fb383dfe" 
                 :starting-sequence-number 173750800000000011849530822N}]
        shard-instance (fn [{:keys [shard-id parent-shard-id starting-sequence-number ending-sequence-number]}]
                         (-> (Shard.)
                             (.withShardId shard-id)
                             (.withParentShardId parent-shard-id)
                             (.withSequenceNumberRange
                              (-> (SequenceNumberRange.)
                                  (.withStartingSequenceNumber (str starting-sequence-number))
                                  (.withEndingSequenceNumber (str ending-sequence-number))))))
        client (reify AmazonDynamoDBStreams
                 (describeStream [client req]
                   (let [drop-n (if (= "full-stream-arn" (.getStreamArn req))
                                  0
                                  (- 2 (swap! describe-stream-invocations inc)))]
                     (-> (DescribeStreamResult.)
                         (.withStreamDescription
                          (-> (StreamDescription.)
                              (.withShards (->> shards
                                                (drop-last drop-n)
                                                (mapv shard-instance))))))))
                 (getShardIterator [client req]
                   (proxy [GetShardIteratorResult] []
                     (getShardIterator []
                       (str "iterator-" (.getShardId req)))))
                 (getRecords [client req]
                   (let [iter (.getShardIterator req)]
                     (proxy [GetRecordsResult] []
                       (getRecords []
                         (let [{:keys [starting-sequence-number ending-sequence-number]}
                               (some #(when (= iter (str "iterator-" (:shard-id %))) %) shards)
                               ending-sequence-number (or ending-sequence-number
                                                          (inc starting-sequence-number))]
                           (->> (range starting-sequence-number ending-sequence-number)
                                (map #(-> (Record.)
                                          (.withDynamodb
                                           (-> (StreamRecord.)
                                               (.withSequenceNumber (str %))))))))
                         )
                       (getNextShardIterator []
                         nil)))))]

    (testing "single-pass-reader"
      (let [record-ch (async/chan 100)
            stream-reader
            (galvani/start-reader
             (galvani/single-pass-reader client
                                        "full-stream-arn"
                                        :trim-horizon
                                        record-ch
                                        (fn [ex] (throw ex))))
            result (async/<!! (async/into [] record-ch))]
        
        (is (= 6 (count result)))))

    (testing "continuous-reader"
      (let [record-ch (async/chan 100)
            state-ch (async/chan 100)
            stream-reader
            (galvani/start-reader
             (galvani/continuous-reader client
                                       "stream-arn"
                                       :latest
                                       record-ch
                                       state-ch))]
        
        (is (= :start (:status (async/<!! state-ch))))
        (let [second-update (async/<!! state-ch)]
          (is (= :update-stream-description (:status second-update)))
          (is (empty? (:processed-shard-ids (:state second-update)))))
        
        (let [third-update (async/<!! state-ch)]
          (is (= :update-stream-description (:status third-update)))
          (is (not (empty? (:processed-shard-ids (:state third-update)))))
          
          (galvani/stop-reader stream-reader))))

    (testing "reader with no-op-parser"
      (let [test-timeout-ch (async/timeout 500)
            record-ch (async/chan 100)
            stream-reader
            (galvani/start-reader
             (galvani/single-pass-reader client
                                        "latest-stream-arn"
                                        :trim-horizon
                                        record-ch
                                        (fn [ex] (throw ex))
                                        {:record-parser (record-parsing/no-op-parser)}))
            [result ch] (async/alts!! [test-timeout-ch record-ch])]
        (is (not= test-timeout-ch ch))
        (is (instance? com.amazonaws.services.dynamodbv2.model.Record result))
        (galvani/stop-reader stream-reader)))))
