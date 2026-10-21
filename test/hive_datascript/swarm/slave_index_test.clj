(ns hive-datascript.swarm.slave-index-test
  "The delta-maintained slave index must always equal a full scan.
   Each test owns a fresh in-memory conn and passes it explicitly."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datascript.core :as d]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.slave-index :as idx]))

(def ^:private ids ["a" "b" "c" "d"])

(def ^:private gen-op
  (gen/one-of
   [(gen/fmap (fn [[id st]] [:upsert id st])
              (gen/tuple (gen/elements ids) (gen/elements [:idle :working :zombie])))
    (gen/fmap (fn [id] [:retract id]) (gen/elements ids))
    (gen/fmap (fn [id] [:parent id]) (gen/elements ids))
    (gen/return [:unrelated])]))

(defn- run-op! [c [op id st]]
  (case op
    :upsert (d/transact! c [{:slave/id id :slave/status st
                             :slave/last-active-at (System/currentTimeMillis)}])
    :retract (when-let [e (d/entity @c [:slave/id id])]
               (d/transact! c [[:db.fn/retractEntity (:db/id e)]]))
    :parent (when (and (d/entity @c [:slave/id id]) (d/entity @c [:slave/id "a"]))
              (d/transact! c [{:slave/id id :slave/parent [:slave/id "a"]}]))
    :unrelated (d/transact! c [{:task/id (str (random-uuid))}])))

(defspec index-equals-scan-after-any-transactions 60
  (prop/for-all [ops (gen/vector gen-op 0 25)]
    (let [c (conn/create-conn)]
      (idx/slaves c @c)
      (doseq [op ops] (run-op! c op))
      (= (idx/scan @c) (idx/slaves c @c)))))

(deftest slave-free-transaction-keeps-the-index-without-rescan
  (let [c (conn/create-conn)]
    (d/transact! c [{:slave/id "a" :slave/status :idle}])
    (let [before (idx/slaves c @c)]
      (d/transact! c [{:task/id "t1"}])
      (testing "the same map instance is served: the delta advanced the basis"
        (is (identical? before (idx/slaves c @c)))))))

(deftest replaced-connection-is-rescanned
  (let [c1 (conn/create-conn)
        c2 (conn/create-conn)]
    (d/transact! c1 [{:slave/id "old" :slave/status :idle}])
    (d/transact! c2 [{:slave/id "new" :slave/status :idle}])
    (idx/slaves c1 @c1)
    (is (= #{"new"} (set (map :slave/id (vals (idx/slaves c2 @c2))))))))

(deftest subscribers-hear-slave-changes-only
  (let [c (conn/create-conn)
        calls (atom 0)]
    (idx/slaves c @c)
    (idx/subscribe! ::probe #(swap! calls inc))
    (try
      (d/transact! c [{:task/id "t1"}])
      (is (= 0 @calls) "a slave-free transaction is silent")
      (d/transact! c [{:slave/id "a" :slave/status :working}])
      (is (= 1 @calls) "a slave change notifies")
      (finally (idx/unsubscribe! ::probe)))
    (d/transact! c [{:slave/id "a" :slave/status :idle}])
    (is (= 1 @calls) "unsubscribed probes are not called")))

(deftest a-failing-subscriber-does-not-break-the-transaction
  (let [c (conn/create-conn)]
    (idx/slaves c @c)
    (idx/subscribe! ::boom #(throw (ex-info "boom" {})))
    (try
      (d/transact! c [{:slave/id "a" :slave/status :idle}])
      (is (= #{"a"} (set (map :slave/id (vals (idx/slaves c @c))))))
      (finally (idx/unsubscribe! ::boom)))))
