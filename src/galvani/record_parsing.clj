(ns galvani.record-parsing
  (:require [clojure.string :as s])
  (:import [com.amazonaws.services.dynamodbv2.model AttributeValue  StreamRecord Record]))


;; from taoensso.faraday
(defn convert-number [^String str]
  (if (.contains str ".")
    (BigDecimal. str)
    (bigint (BigInteger. str))))

(defn normalize-attribute-value [^AttributeValue attr]
  (or
   (some-> (.getS attr) (vector :s))
   (some-> (.getN attr) (vector :n))
   (some-> (.getNULL attr) (vector :null))
   (some-> (.getBOOL attr) (vector :bool))
   (some-> (.getSS attr) (vector :ss))
   (some-> (.getNS attr) (vector :ns))
   (some-> (.getBS attr) (vector :bs))
   (some-> (.getB attr) (vector :b))
   (some-> (.getM attr) (vector :m))
   (some-> (.getL attr) (vector :l))))

(defn normalize-attribute-map [attr]
  (let [[type x] (first attr)]
    [x (-> type name s/lower-case keyword)]))

(defn convert-attribute
  [converter attr]
  (let [[val type]
        (if (instance? AttributeValue attr)
          (normalize-attribute-value attr)
          ;; parsed json records use plain maps
          (normalize-attribute-map attr))]
    (converter type val)))


(defn ddb-item->clj [item attribute-converter]
  (reduce-kv (fn [m k v]
               (assoc m
                      (keyword k)
                      (convert-attribute attribute-converter v)))
             {}
             (into {} item)))

(defn stream-record->clj [^StreamRecord stream-record attribute-converter]
  {:keys (ddb-item->clj (.getKeys stream-record) attribute-converter)
   :new-image (ddb-item->clj (.getNewImage stream-record) attribute-converter)
   :old-image (ddb-item->clj (.getOldImage stream-record) attribute-converter)
   :sequence-number (.getSequenceNumber stream-record)
   :size-bytes (.getSizeBytes stream-record)
   :stream-view-type (.getStreamViewType stream-record)})

(defn record->clj [^Record record iterator-info attribute-converter]
  {:iterator-info iterator-info
   :event-id (.getEventID record)
   :event-name (.getEventName record)
   :event-version (.getEventVersion record)
   :event-source (.getEventSource record)
   :aws-region (.getAwsRegion record)
   :dynamodb (stream-record->clj (.getDynamodb record) attribute-converter)})

(defn default-attribute-converter [type val]
  (case type
    :s val
    :n (convert-number val)
    :null nil
    :bool (boolean val)
    :ss (into #{} val)
    :ns (into #{} (mapv convert-number val))
    :bs (into #{} val)
    :b val
    :l (mapv default-attribute-converter val)
    :m (zipmap (mapv keyword (.keySet ^java.util.Map val))
               (mapv default-attribute-converter (.values ^java.util.Map val)))))
