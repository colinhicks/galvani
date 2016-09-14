(ns faraday-streams.client-test
  (:require [faraday-streams.client :as client]
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
