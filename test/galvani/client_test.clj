(ns galvani.client-test
  (:require [galvani.client :as client]
            [clojure.test :refer [deftest is]]))


(deftest records
  (let [shards (atom ["1000" "2000" "3000"])
        mock-record (fn [shard name]
                      (let [id-val (-> (com.amazonaws.services.dynamodbv2.model.AttributeValue.)
                                       (.withN (str shard name)))
                            name-val (-> (com.amazonaws.services.dynamodbv2.model.AttributeValue.)
                                         (.withS name))]
                        (-> (com.amazonaws.services.dynamodbv2.model.Record.)
                            (.withDynamodb (-> (com.amazonaws.services.dynamodbv2.model.StreamRecord.)
                                               (.withKeys (java.util.HashMap. {"id" id-val}))
                                               (.withNewImage (java.util.HashMap. {"id" id-val
                                                                                   "name" name-val})))))))
        mock-client (reify com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams
                      (getRecords [client req]
                        (proxy [com.amazonaws.services.dynamodbv2.model.GetRecordsResult] []
                          (getRecords []
                            (mapv (partial mock-record (first @shards)) ["100" "101" "102"]))
                          (getNextShardIterator []
                            (first (swap! shards rest))))))]
    (let [recs (client/records mock-client (first @shards))]
      (is (instance? clojure.lang.LazySeq recs))
      (is (= {:name "100" :id 1000100N} (->> recs (take 4) first :dynamodb :new-image)))
      (is (= "2000" (first @shards)))
      (last (take 4 recs))
      (is (= "3000" (first @shards)))
      (is (= {:name "102" :id 3000102N} (->> recs last :dynamodb :new-image)))
      (is (= 9 (count (take 999 recs)))))))


(deftest describe-stream
  (let [shard (fn [[id parent-id start end]] (-> (com.amazonaws.services.dynamodbv2.model.Shard.)
                                                 (.withShardId id)
                                                 (.withParentShardId parent-id)
                                                 (.withSequenceNumberRange
                                                  (-> (com.amazonaws.services.dynamodbv2.model.SequenceNumberRange.)
                                                      (.withStartingSequenceNumber start)
                                                      (.withEndingSequenceNumber end)))))
        shard-params [["shardId-1-p3as6f" nil "10000001" "10000002"]
                      ["shardId-2-3n1xaf" "shardId-1-p3as6f" "10000005" "10000007"]
                      ["shardId-3-v13n3d" "shardId-2-3n1xaf" "10000008" "10000009"]]
        shards (mapv shard shard-params)
        mock-client (reify com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams
                      (describeStream [client req]
                        (-> (com.amazonaws.services.dynamodbv2.model.DescribeStreamResult.)
                            (.withStreamDescription
                             (-> (com.amazonaws.services.dynamodbv2.model.StreamDescription.)
                                 (.withShards shards))))))
        stream-description (client/describe-stream mock-client nil)]
    (is (= (mapv first shard-params) (mapv :shard-id (:shards stream-description))))
    (is (= (take 2 (mapv :shard-id (:shards stream-description)))
           (drop 1 (mapv :parent-shard-id (:shards stream-description)))))
    (client/describe-stream mock-client nil)))
