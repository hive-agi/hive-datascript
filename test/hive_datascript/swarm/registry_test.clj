(ns hive-datascript.swarm.registry-test
  "Tests for hive-datascript.swarm.registry: the ISP-segregated protocol
   records and their default instances, exercised through the public
   record methods (they delegate to lings/queries/coordination).
   Fresh in-memory conn per test via connection/*test-conn*."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datascript.core :as d]
            [hive-spi.swarm.protocol :as proto]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.registry :as registry]
            [hive-datascript.swarm.queries :as queries]
            [hive-datascript.swarm.lings :as lings]))

(defn fresh-conn-fixture [f]
  (conn/with-test-conn (conn/create-conn) f))

(use-fixtures :each fresh-conn-fixture)

(def ^:private reg (registry/get-default-registry))
(def ^:private claim-store (registry/get-default-claim-store))
(def ^:private critical-ops (registry/get-default-critical-ops))
(def ^:private coordination (registry/get-default-coordination))
(def ^:private db (registry/get-default-db))

;;; =============================================================================
;;; ISwarmRegistry
;;; =============================================================================

(deftest registry-slave-lifecycle-test
  (testing "add/get/update/remove through the registry record"
    (proto/add-slave! reg "s-1" {:name "one" :status :idle})
    (let [s (proto/get-slave reg "s-1")]
      (is (= "s-1" (:slave/id s)))
      (is (= "one" (:slave/name s))))
    (proto/update-slave! reg "s-1" {:slave/status :working})
    (is (= :working (:slave/status (proto/get-slave reg "s-1"))))
    (is (= ["s-1"] (mapv :slave/id (proto/get-all-slaves reg))))
    (is (= ["s-1"] (mapv :slave/id (proto/get-slaves-by-status reg :working))))
    (proto/remove-slave! reg "s-1")
    (is (nil? (proto/get-slave reg "s-1")))
    (is (empty? (proto/get-all-slaves reg)))))

(deftest registry-project-filter-test
  (testing "get-slaves-by-project through the record"
    (proto/add-slave! reg "a" {:project-id "p1"})
    (proto/add-slave! reg "b" {:project-id "p2"})
    (is (= ["a"] (mapv :slave/id (proto/get-slaves-by-project reg "p1"))))))

(deftest registry-task-ops-test
  (testing "add/get/update task + per-slave task listing (1- and 2-arity)"
    (proto/add-slave! reg "s-1" {})
    (proto/add-task! reg "t-1" "s-1" {:prompt "p"})
    (let [t (proto/get-task reg "t-1")]
      (is (= "t-1" (:task/id t)))
      (is (= "s-1" (:task/slave t))))
    (proto/update-task! reg "t-1" {:task/status :completed})
    (is (= :completed (:task/status (proto/get-task reg "t-1"))))
    (is (= ["t-1"] (mapv :task/id (proto/get-tasks-for-slave reg "s-1"))))
    (is (= ["t-1"] (mapv :task/id (proto/get-tasks-for-slave reg "s-1" :completed))))
    (is (empty? (proto/get-tasks-for-slave reg "s-1" :error)))))

;;; =============================================================================
;;; IClaimStore
;;; =============================================================================

(deftest claim-store-lifecycle-test
  (testing "claim / conflict / refresh / release through the claim store"
    (proto/add-slave! reg "s-1" {})
    (proto/add-slave! reg "s-2" {})
    (proto/-claim-file! claim-store "f.clj" "s-1")
    (is (= "s-1" (:slave-id (proto/-get-claims-for-file claim-store "f.clj"))))
    (is (= 1 (count (proto/-get-all-claims claim-store))))
    (is (true? (:conflict? (proto/-has-conflict? claim-store "f.clj" "s-2"))))
    (is (= [{:file "f.clj" :held-by "s-1"}]
           (proto/-check-file-conflicts claim-store "s-2" ["f.clj"])))
    (is (some? (proto/-refresh-claim! claim-store "f.clj")))
    (is (some? (proto/-release-claim! claim-store "f.clj")))
    (is (empty? (proto/-get-all-claims claim-store)))))

(deftest claim-store-batch-release-test
  (testing "release-claims-for-slave! and -for-task! through the store"
    (proto/add-slave! reg "s-1" {})
    (proto/add-task! reg "t-1" "s-1" {})
    (proto/-claim-file! claim-store "a.clj" "s-1" {:task-id "t-1"})
    (proto/-claim-file! claim-store "b.clj" "s-1")
    (is (= 1 (proto/-release-claims-for-task! claim-store "t-1")))
    (is (= 1 (proto/-release-claims-for-slave! claim-store "s-1")))
    (is (empty? (proto/-get-all-claims claim-store)))))

(deftest claim-store-stale-cleanup-test
  (testing "-cleanup-stale-claims! releases only aged claims"
    (proto/add-slave! reg "s-1" {})
    (proto/-claim-file! claim-store "old.clj" "s-1")
    (proto/-claim-file! claim-store "new.clj" "s-1")
    (let [c (conn/ensure-conn)
          eid (:db/id (d/entity @c [:claim/file "old.clj"]))]
      (d/transact! c [{:db/id eid
                       :claim/created-at
                       (java.util.Date. (- (System/currentTimeMillis) 700000))}]))
    (let [res (proto/-cleanup-stale-claims! claim-store 300000)]
      (is (= 1 (:released-count res)))
      (is (= ["old.clj"] (:released-files res))))))

(deftest claim-store-wait-queue-and-history-test
  (testing "wait-queue add and claim-history archive/read via the store"
    (proto/add-slave! reg "s-1" {})
    (proto/-add-to-wait-queue! claim-store "s-1" "f.clj")
    (is (some? (proto/-add-to-wait-queue! claim-store "s-1" "f.clj"))
        "upsert does not throw on repeat")
    (proto/-archive-claim-to-history! claim-store "f.clj"
                                      {:slave-id "s-1" :prior-hash "h"})
    (let [hist (proto/-get-recent-claim-history claim-store {:file "f.clj"})]
      (is (= ["f.clj"] (mapv :file hist))))))

;;; =============================================================================
;;; ICriticalOps
;;; =============================================================================

(deftest critical-ops-record-test
  (testing "enter/get/exit + can-kill? through the record"
    (proto/add-slave! reg "s-1" {})
    (proto/-enter-critical-op! critical-ops "s-1" :commit)
    (is (= #{:commit} (proto/-get-critical-ops critical-ops "s-1")))
    (is (false? (:can-kill? (proto/-can-kill? critical-ops "s-1"))))
    (proto/-exit-critical-op! critical-ops "s-1" :commit)
    (is (true? (:can-kill? (proto/-can-kill? critical-ops "s-1"))))
    (is (= #{} (proto/-get-critical-ops critical-ops "ghost")))))

;;; =============================================================================
;;; ICoordination (wrap queue + coordinators + session registry delegates)
;;; =============================================================================

(deftest coordination-wrap-queue-test
  (testing "add/list/mark-processed through the coordination record"
    (proto/-add-wrap-notification! coordination "w-1" {:project-id "p1"})
    (is (= ["w-1"] (mapv :wrap-queue/id (proto/-get-unprocessed-wraps coordination))))
    (is (= ["w-1"] (mapv :wrap-queue/id (proto/-get-unprocessed-wraps-for-project coordination "p1"))))
    (proto/-mark-wrap-processed! coordination "w-1")
    (is (empty? (proto/-get-unprocessed-wraps coordination)))))

(deftest coordination-coordinator-test
  (testing "register/heartbeat/terminate/remove through the record"
    (proto/-register-coordinator! coordination "c-1" {:project "p1"})
    (let [c (proto/-get-coordinator coordination "c-1")]
      (is (= "c-1" (:coordinator/id c)))
      (is (= :active (:coordinator/status c))))
    (proto/-update-heartbeat! coordination "c-1")
    (is (= :active (:coordinator/status (proto/-get-coordinator coordination "c-1"))))
    (is (= ["c-1"] (mapv :coordinator/id (proto/-get-all-coordinators coordination))))
    (is (= ["c-1"] (mapv :coordinator/id (proto/-get-coordinators-for-project coordination "p1"))))
    (proto/-mark-coordinator-terminated! coordination "c-1")
    (is (= :terminated (:coordinator/status (proto/-get-coordinator coordination "c-1"))))
    (proto/-remove-coordinator! coordination "c-1")
    (is (nil? (proto/-get-coordinator coordination "c-1")))))

(deftest coordination-session-registry-test
  (testing "completed tasks + kanban movements + scoped clears via the record"
    (proto/-register-completed-task! coordination "task-1"
                                     {:title "T" :project-id "p1" :session-id "sess-1"})
    (is (= ["task-1"] (mapv :completed-task/id
                            (proto/-get-completed-tasks-this-session coordination))))
    (is (= ["task-1"] (mapv :completed-task/id
                            (proto/-get-completed-tasks-this-session
                             coordination {:project-id "p1"}))))
    (is (empty? (proto/-get-completed-tasks-this-session
                 coordination {:agent-id "nobody"})))
    ;; scoped clear keeps unharvested rows
    (proto/-register-completed-task! coordination "task-2" {})
    (is (= 1 (proto/-clear-completed-tasks! coordination ["task-1"])))
    (is (= ["task-2"] (mapv :completed-task/id
                            (proto/-get-completed-tasks-this-session coordination))))
    (is (= 1 (proto/-clear-completed-tasks! coordination))))
  (testing "kanban movements through the record"
    ;; src :pre requires string task-id and :to
    (proto/-register-kanban-movement! coordination
                                      {:task-id "task-1" :from "todo" :to "done"
                                       :project-id "p1" :session-id "sess-1"})
    (let [mvs (proto/-get-kanban-movements-this-session coordination)]
      (is (= 1 (count mvs)))
      (is (= "task-1" (:kanban-movement/task-id (first mvs)))))
    (is (= 1 (proto/-clear-kanban-movements! coordination)))
    (is (empty? (proto/-get-kanban-movements-this-session coordination)))))

;;; =============================================================================
;;; ISwarmDb
;;; =============================================================================

(deftest swarm-db-record-test
  (testing "transact!/current-db/stats/listen/reset"
    (proto/-transact! db [{:db/id -1 :slave/id "raw-1" :slave/name "raw"}])
    (let [current (proto/-current-db db)]
      (is (some? (d/entity current [:slave/id "raw-1"]))))
    (let [stats (proto/-db-stats db)]
      (is (= 1 (:slaves stats))))
    ;; -listen! returns the listener key it registered
    (is (= :test-listener (proto/-listen! db :test-listener (fn [_ _]))))
    (proto/-unlisten! db :test-listener)
    ;; -reset-db! delegates to conn/reset-conn! which resets the GLOBAL conn
    ;; atom only; the *test-conn* binding stays in scope, so the current
    ;; (test) db is unaffected by the reset.
    (proto/-reset-db! db)
    (is (some? (d/entity (proto/-current-db db) [:slave/id "raw-1"]))
        "reset-conn! resets the global atom, not the bound test-conn")))

;;; =============================================================================
;;; Property: registry delegates stay consistent with the underlying store
;;; =============================================================================

(deftest registry-roundtrip-property-test
  "Property: for any generated slave opt maps, going through the
   ISwarmRegistry record produces exactly the same read-back as going
   through the underlying queries/get-slave, and removal retracts."
  []
  (dotimes [i 100]
    (let [id (str "rr-" i)
          opts {:name (str "n" i)
                :status (rand-nth [:idle :working :blocked :error])
                :depth (inc (rand-int 4))
                :project-id (str "proj-" (rand-int 5))}]
      (proto/add-slave! reg id opts)
      (is (= (queries/get-slave id) (proto/get-slave reg id))
          (str "mismatch @" i))
      (proto/remove-slave! reg id)
      (is (nil? (queries/get-slave id)) (str "retracted @" i)))))
