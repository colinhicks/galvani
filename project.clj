(defproject net.colinhicks/galvani "0.0.1"
  :description "A Clojure client for reading DynamoDB Streams"
  :url "https://github.com/colinhicks/galvani"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.11.24"]
                 [org.clojure/core.async "0.2.391"]
                 [com.stuartsierra/dependency "0.2.0"]])

