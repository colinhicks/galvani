(ns galvani.client-test
  (:require [galvani.client :as client]
            [clojure.test :refer [deftest is]]
            [clojure.core.async :as async])
  (:import [com.amazonaws.services.dynamodbv2 AmazonDynamoDBStreams]
           [com.amazonaws.services.dynamodbv2.model Record StreamRecord AttributeValue
            GetRecordsResult Shard SequenceNumberRange DescribeStreamResult StreamDescription]))


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
                            (first (swap! iters rest))))))]
    (let [recs (client/read-shard mock-client {:iterator (first @iters)})]
      (is (instance? clojure.lang.LazySeq recs))
      (is (= {:name "100" :id 1000100N} (->> recs (take 4) first :dynamodb :new-image)))
      (is (= "2000" (first @iters)))
      (last (take 4 recs))
      (is (= "3000" (first @iters)))
      (is (= {:name "102" :id 3000102N} (->> recs last :dynamodb :new-image)))
      (is (= 9 (count (take 999 recs)))))))


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
                              (str iter))))))]
    (let [recs (client/read-shard mock-client {:iterator (str (first @iters))})]
      (is (= expected-count (count recs))))))


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
        stream-description (client/describe-stream mock-client nil)]
    (is (= (mapv first shard-params) (mapv :shard-id (:shards stream-description))))
    (is (= (take 2 (mapv :shard-id (:shards stream-description)))
           (drop 1 (mapv :parent-shard-id (:shards stream-description)))))
    (client/describe-stream mock-client nil)))


(deftest read-shards
  (let [shards [[["af234" :a :b] ["asfe3" :c]]
                [["vwe23"] ["32nfs"] ["v4245" :d]]
                [["pa5fz" :e :f]]]
        mock-record (fn [iter name]
                      (let [id-val (-> (AttributeValue.) (.withS (str iter "-" name)))
                            name-val (-> (AttributeValue.) (.withS name))]
                        (-> (Record.)
                            (.withDynamodb
                             (-> (StreamRecord.)
                                 (.withKeys (java.util.HashMap. {"id" id-val}))
                                 (.withNewImage (java.util.HashMap. {"id" id-val
                                                                     "name" name-val})))))))
        mock-client (reify AmazonDynamoDBStreams
                      (getRecords [client req]
                        (let [iter (.getShardIterator req)]
                          (proxy [GetRecordsResult] []
                            (getRecords []
                              (->> shards
                                   (mapcat identity)
                                   (some (fn [[xiter & xs]]
                                           (when (= iter xiter)
                                             (mapv (comp
                                                    (partial mock-record xiter)
                                                    name)
                                                   xs)))))
                              )
                            (getNextShardIterator []
                              (->> shards
                                   (some (fn [sh]
                                          (->> sh
                                               (drop-while #(not= iter (first %)))
                                               next
                                               ffirst)))))))))
        iterator-infos (map-indexed (fn [i x] {:iterator (ffirst x)
                                               :shard-id (str "shard-id-" i)}) shards)
        ch (client/read-shards mock-client iterator-infos {})
        results (async/<!! (async/into [] ch))]
    (is (= #{"shard-id-0" "shard-id-1" "shard-id-2"}
           (set (map (comp :shard-id :iterator-info) results))))
    (is (= #{"pa5fz-f" "pa5fz-e" "v4245-d" "asfe3-c" "af234-b" "af234-a"}
           (set (map (comp :id :keys :dynamodb) results)))))
  )


