(ns hive-datascript.swarm.lings-test
  "Tests for hive-datascript.swarm.lings: slave/task/claim lifecycle.
   Every test runs against a FRESH in-memory DataScript conn via the
   connection/*test-conn* isolation override (see connection.clj)."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datascript.core :as d]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.lings :as lings]
            [hive-datascript.swarm.queries :as queries]
            [hive-spi.swarm.ports.events :as events-port]))

(defn fresh-conn-fixture
  "Bind a fresh in-memory conn per test; clear host hooks so no stale
   state leaks between tests."
  [f]
  (conn/with-test-conn (conn/create-conn) f))

(use-fixtures :each fresh-conn-fixture)

;;; =============================================================================
;;; Slave CRUD
;;; =============================================================================

(deftest add-and-get-slave-test
  (testing "add-slave! stamps defaults and get-slave returns the row"
    (lings/add-slave! "s-1" {:name "ling one" :cwd "/tmp/x" :project-id "p1"})
    (let [s (queries/get-slave "s-1")]
      (is (some? s))
      (is (= "s-1" (:slave/id s)))
      (is (= "ling one" (:slave/name s)))
      (is (= :idle (:slave/status s)))
      (is (= 1 (:slave/depth s)))
      (is (= 0 (:slave/tasks-completed s)))
      (is (true? (:slave/alive? s)))
      (is (some? (:slave/last-active-at s)))
      (is (= "/tmp/x" (:slave/cwd s)))
      (is (= "p1" (:slave/project-id s))))))

(deftest add-slave-presets-and-parent-test
  (testing "presets are stored as a vector, parent resolves to the id"
    (lings/add-slave! "parent" {})
    (lings/add-slave! "child" {:presets ["a" "b"] :parent "parent"})
    (let [c (queries/get-slave "child")]
      (is (= #{"a" "b"} (set (:slave/presets c)))) ; cardinality/many → set on read
      (is (= "parent" (:slave/parent c))))))

(deftest update-slave-test
  (testing "update-slave! applies updates and returns nil for unknown id"
    (lings/add-slave! "s-1" {})
    (lings/update-slave! "s-1" {:slave/status :working :slave/depth 3})
    (let [s (queries/get-slave "s-1")]
      (is (= :working (:slave/status s)))
      (is (= 3 (:slave/depth s))))
    (is (nil? (lings/update-slave! "nope" {:slave/status :working})))))

(deftest remove-slave-test
  (testing "remove-slave! retracts the entity; nil when already gone"
    (lings/add-slave! "s-1" {})
    (lings/remove-slave! "s-1")
    (is (nil? (queries/get-slave "s-1")))
    (is (nil? (lings/remove-slave! "s-1")))))

(deftest remove-slave-releases-claims-test
  (testing "removing a slave releases its file claims"
    (lings/add-slave! "s-1" {})
    (lings/claim-file! "f.clj" "s-1")
    (is (some? (queries/get-claims-for-file "f.clj")))
    (lings/remove-slave! "s-1")
    (is (nil? (queries/get-claims-for-file "f.clj")))))

;;; =============================================================================
;;; Critical ops guard
;;; =============================================================================

(deftest critical-ops-guard-test
  (testing "enter/exit critical ops toggle can-kill?"
    (lings/add-slave! "s-1" {})
    (is (true? (:can-kill? (lings/can-kill? "s-1"))))
    (lings/enter-critical-op! "s-1" :wrap)
    (is (= #{:wrap} (lings/get-critical-ops "s-1")))
    (is (false? (:can-kill? (lings/can-kill? "s-1"))))
    (is (= #{:wrap} (:blocking-ops (lings/can-kill? "s-1"))))
    (lings/exit-critical-op! "s-1" :wrap)
    (is (true? (:can-kill? (lings/can-kill? "s-1"))))))

(deftest with-critical-op-macro-test
  (testing "guard released even when body throws"
    (lings/add-slave! "s-1" {})
    (lings/with-critical-op "s-1" :commit
      (is (false? (:can-kill? (lings/can-kill? "s-1")))))
    (is (true? (:can-kill? (lings/can-kill? "s-1"))))
    (is (thrown? Exception
                 (lings/with-critical-op "s-1" :dispatch
                   (throw (ex-info "boom" {})))))
    (is (true? (:can-kill? (lings/can-kill? "s-1"))))))

;;; =============================================================================
;;; Task lifecycle
;;; =============================================================================

(deftest add-task-test
  (testing "add-task! wires the slave ref and default status"
    (lings/add-slave! "s-1" {})
    (lings/add-task! "t-1" "s-1" {:prompt "do it" :files ["a.clj"]})
    (let [t (queries/get-task "t-1")]
      (is (= "t-1" (:task/id t)))
      (is (= "s-1" (:task/slave t)))
      (is (= :dispatched (:task/status t)))
      (is (= "do it" (:task/prompt t)))
      (is (= #{"a.clj"} (set (:task/files t)))))))

(deftest complete-task-test
  (testing "complete-task! bumps slave stats and releases task claims"
    (lings/add-slave! "s-1" {})
    (lings/add-task! "t-1" "s-1" {})
    (lings/update-slave! "s-1" {:slave/current-task "t-1"})
    (lings/claim-file! "f.clj" "s-1" {:task-id "t-1"})
    (lings/complete-task! "t-1")
    (let [t (queries/get-task "t-1")
          s (queries/get-slave "s-1")]
      (is (= :completed (:task/status t)))
      (is (= 1 (:slave/tasks-completed s)))
      (is (nil? (:slave/current-task s))
          "current-task ref is cleared on completion"))
    (is (nil? (queries/get-claims-for-file "f.clj"))
        "task claims released on completion")
    (is (nil? (lings/complete-task! "missing")))))

(deftest fail-task-test
  (testing "fail-task! records the failure status and clears current-task"
    (lings/add-slave! "s-1" {})
    (lings/add-task! "t-1" "s-1" {})
    (lings/update-slave! "s-1" {:slave/current-task "t-1"})
    (lings/fail-task! "t-1" :error)
    (is (= :error (:task/status (queries/get-task "t-1"))))
    (is (nil? (:slave/current-task (queries/get-slave "s-1"))))
    (is (thrown? AssertionError (lings/fail-task! "t-2" :bogus)))))

(deftest update-task-test
  (testing "update-task! applies :task/* updates, nil for unknown"
    (lings/add-slave! "s-1" {})
    (lings/add-task! "t-1" "s-1" {})
    (lings/update-task! "t-1" {:task/status :queued})
    (is (= :queued (:task/status (queries/get-task "t-1"))))
    (is (nil? (lings/update-task! "nope" {:task/status :queued})))))

;;; =============================================================================
;;; Claims
;;; =============================================================================

(deftest claim-file-test
  (testing "claim stores holder, task ref (only when the task exists), prior-hash"
    (lings/add-slave! "s-1" {})
    (lings/add-task! "t-1" "s-1" {})
    (lings/claim-file! "f.clj" "s-1" {:task-id "t-1" :prior-hash "abc12345"})
    (let [c (queries/get-claims-for-file "f.clj")]
      (is (= "s-1" (:slave-id c)))
      (is (= "t-1" (:task-id c)))
      (is (= "abc12345" (:prior-hash c)))
      (is (some? (:created-at c))))
    ;; phantom task id: no entity -> no ref, but the claim itself still exists
    (lings/claim-file! "g.clj" "s-1" {:task-id "phantom"})
    (let [c (queries/get-claims-for-file "g.clj")]
      (is (= "s-1" (:slave-id c)))
      (is (nil? (:task-id c))))))

(deftest release-claim-test
  (testing "release-claim! removes the claim; nil when unclaimed"
    (lings/add-slave! "s-1" {})
    (lings/claim-file! "f.clj" "s-1")
    (is (some? (lings/release-claim! "f.clj")))
    (is (nil? (queries/get-claims-for-file "f.clj")))
    (is (nil? (lings/release-claim! "f.clj")))))

(deftest release-claims-for-task-test
  (testing "batch release by task"
    (lings/add-slave! "s-1" {})
    (lings/add-task! "t-1" "s-1" {})
    (lings/claim-file! "a.clj" "s-1" {:task-id "t-1"})
    (lings/claim-file! "b.clj" "s-1" {:task-id "t-1"})
    (lings/claim-file! "c.clj" "s-1")
    (is (= 2 (lings/release-claims-for-task! "t-1")))
    (is (nil? (queries/get-claims-for-file "a.clj")))
    (is (nil? (queries/get-claims-for-file "b.clj")))
    (is (some? (queries/get-claims-for-file "c.clj")))))

(defn- recording-events
  "An events port stub whose dispatch! records every event vector into
   SEEN (an atom). Every handler counts as registered."
  [seen]
  (reify events-port/IEventDispatch
    (dispatch! [_ event-v] (swap! seen conj event-v) event-v)
    (handler-registered? [_ _event-id] true)))

(defn- released-events
  "Run F with a recording events port installed; return the
   :claim/file-released payloads it dispatched, keyed by :file."
  [f]
  (let [seen (atom [])]
    (events-port/set-events! (recording-events seen))
    (try
      (f)
      (finally (events-port/clear-events!)))
    (into {}
          (keep (fn [[id payload]]
                  (when (= :claim/file-released id)
                    [(:file payload) payload])))
          @seen)))

(deftest release-reports-the-holder-as-released-by-test
  (testing "release-claim! names the slave that held the claim"
    (lings/add-slave! "s-1" {})
    (lings/claim-file! "f.clj" "s-1")
    (let [evs (released-events #(lings/release-claim! "f.clj"))]
      (is (= {:file "f.clj" :released-by "s-1"} (get evs "f.clj")))))
  (testing "complete-task! releases through release-claims-for-task!, sender is the holder"
    (lings/add-slave! "s-2" {})
    (lings/add-task! "t-2" "s-2" {})
    (lings/claim-file! "a.clj" "s-2" {:task-id "t-2"})
    (lings/claim-file! "b.clj" "s-2" {:task-id "t-2"})
    (let [evs (released-events #(lings/complete-task! "t-2"))]
      (is (= #{"a.clj" "b.clj"} (set (keys evs))))
      (is (every? #(= "s-2" (:released-by %)) (vals evs)))))
  (testing "release-claims-for-slave! names that slave on every release"
    (lings/add-slave! "s-3" {})
    (lings/claim-file! "c.clj" "s-3")
    (lings/claim-file! "d.clj" "s-3")
    (let [evs (released-events #(lings/release-claims-for-slave! "s-3"))]
      (is (= {"c.clj" {:file "c.clj" :released-by "s-3"}
              "d.clj" {:file "d.clj" :released-by "s-3"}}
             evs)))))

(deftest stale-claim-cleanup-test
  (testing "cleanup-stale-claims! releases only claims older than threshold"
    (lings/add-slave! "s-1" {})
    (lings/claim-file! "old.clj" "s-1")
    (lings/claim-file! "fresh.clj" "s-1")
    ;; backdate the first claim directly on the test conn
    (let [c (conn/ensure-conn)
          eid (:db/id (d/entity @c [:claim/file "old.clj"]))]
      (d/transact! c [{:db/id eid
                       :claim/created-at (java.util.Date.
                                          (- (System/currentTimeMillis) 600000))}]))
    (let [res (lings/cleanup-stale-claims! 300000)]
      (is (= 1 (:released-count res)))
      (is (= ["old.clj"] (:released-files res))))
    (is (nil? (queries/get-claims-for-file "old.clj")))
    (is (some? (queries/get-claims-for-file "fresh.clj")))))

(deftest refresh-claim-test
  (testing "refresh-claim! stamps a heartbeat, nil for unclaimed"
    (lings/add-slave! "s-1" {})
    (lings/claim-file! "f.clj" "s-1")
    (is (some? (lings/refresh-claim! "f.clj")))
    (let [c (conn/ensure-conn)]
      (is (some? (:claim/heartbeat-at
                  (d/entity @c [:claim/file "f.clj"])))))
    (is (nil? (lings/refresh-claim! "nope.clj")))))

;;; =============================================================================
;;; Wait queue + claim history
;;; =============================================================================

(deftest wait-queue-test
  (testing "add-to-wait-queue! is an upsert keyed on ling+file"
    (lings/add-to-wait-queue! "l-1" "f.clj")
    (lings/add-to-wait-queue! "l-1" "f.clj")
    (let [c (conn/ensure-conn)
          n (count (d/q '[:find ?e :where [?e :wait-queue/id _]] @c))]
      (is (= 1 n) "duplicate wait entry collapses to one row"))))

(deftest archive-claim-to-history-test
  (testing "archive records hashes and line deltas"
    (lings/archive-claim-to-history! "f.clj"
                                     {:slave-id "s-1"
                                      :prior-hash "aaa"
                                      :released-hash "bbb"
                                      :lines-added 3
                                      :lines-removed 1})
    (let [h (first (queries/get-recent-claim-history :file "f.clj"))]
      (is (= "f.clj" (:file h)))
      (is (= "s-1" (:slave-id h)))
      (is (= "aaa" (:prior-hash h)))
      (is (= "bbb" (:released-hash h)))
      (is (= 3 (:lines-added h)))
      (is (= 1 (:lines-removed h))))))

;;; =============================================================================
;;; Stdout ring buffer (pure atom state, no conn involved)
;;; =============================================================================

(deftest stdout-ring-buffer-test
  (testing "append/read/since round-trip and cleanup"
    (lings/reset-stdout-buffers!)
    (try
      (is (= 1 (lings/append-stdout! "l-1" "one")))
      (is (= 3 (lings/append-stdout! "l-1" ["two" "three"])))
      (let [all (lings/get-stdout "l-1")]
        (is (= ["one" "two" "three"] (mapv :text all)))
        (is (= [0 1 2] (mapv :idx all))))
      (is (= ["three"] (mapv :text (lings/get-stdout "l-1" 1))))
      (is (= ["two" "three"] (mapv :text (lings/get-stdout-since "l-1" 0))))
      (is (= ["three"] (mapv :text (lings/get-stdout-since "l-1" 1))))
      (is (= [] (lings/get-stdout-since "l-1" 2)))
      (is (= 3 (:line-count (lings/get-stdout-buffer-info "l-1"))))
      (is (true? (lings/cleanup-stdout-buffer! "l-1")))
      (is (false? (lings/cleanup-stdout-buffer! "l-1")))
      (is (= [] (lings/get-stdout "l-1")))
      (finally
        (lings/reset-stdout-buffers!)))))

;;; =============================================================================
;;; Property: add/update round-trips never lose identity (generative, no deps)
;;; =============================================================================

(defn- rand-status []
  (rand-nth [:idle :spawning :starting :initializing :working :blocked]))

(defn- rand-slave-opts [i]
  {:name (str "ling " i)
   :status (rand-status)
   :depth (inc (rand-int 5))
   :cwd (str "/tmp/ws-" (rand-int 100))
   :project-id (str "p-" (rand-int 10))})

(deftest slave-roundtrip-property-test
  "Property: for any generated (name, status, depth, cwd, project),
   add-slave! followed by get-slave preserves every stored attribute,
   and update-slave! overwrites only the keys given."
  []
  (dotimes [i 200]
    (let [id (str "prop-" i)
          opts (rand-slave-opts i)]
      (lings/add-slave! id opts)
      (let [got (queries/get-slave id)]
        (is (= (:name opts) (:slave/name got)) (str "name @" i))
        (is (= (:status opts) (:slave/status got)) (str "status @" i))
        (is (= (:depth opts) (:slave/depth got)) (str "depth @" i))
        (is (= (:cwd opts) (:slave/cwd got)) (str "cwd @" i))
        (is (= (:project-id opts) (:slave/project-id got)) (str "project @" i)))
      (lings/update-slave! id {:slave/status :terminated})
      (is (= :terminated (:slave/status (queries/get-slave id))))
      (lings/remove-slave! id)
      (is (nil? (queries/get-slave id)) (str "retracted @" i)))))
