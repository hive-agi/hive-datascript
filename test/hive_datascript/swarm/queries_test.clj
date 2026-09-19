(ns hive-datascript.swarm.queries-test
  "Tests for hive-datascript.swarm.queries: read paths + filters.
   Fresh in-memory conn per test via connection/*test-conn*."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [datascript.core :as d]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.lings :as lings]
            [hive-datascript.swarm.queries :as queries]))

(defn fresh-conn-fixture [f]
  (conn/with-test-conn (conn/create-conn) f))

(use-fixtures :each fresh-conn-fixture)

(defn- seed-slave! [id & [{:keys [status project parent depth presets kanban-task-id stale?]}]]
  (lings/add-slave! id (cond-> {:status (or status :idle)}
                         project (assoc :project-id project)
                         parent (assoc :parent parent)
                         depth (assoc :depth depth)
                         presets (assoc :presets presets)
                         kanban-task-id (assoc :kanban-task-id kanban-task-id)))
  (when stale?
    (let [c (conn/ensure-conn)
          eid (:db/id (d/entity @c [:slave/id id]))]
      (d/transact! c [{:db/id eid
                       :slave/last-active-at
                       (- (System/currentTimeMillis) 3600000)}]))))

(defn- seed-task! [task-id slave-id & [{:keys [status]}]]
  (lings/add-task! task-id slave-id {:status (or status :dispatched)}))

;;; =============================================================================
;;; Slave lookups
;;; =============================================================================

(deftest get-slave-missing-test
  (testing "unknown id returns nil"
    (is (nil? (queries/get-slave "ghost")))))

(deftest get-all-slaves-filters-stale-test
  (testing "default read hides stale rows; :include-stale? surfaces them"
    (seed-slave! "fresh")
    (seed-slave! "stale-row" {:stale? true})
    (let [default (queries/get-all-slaves)
          all (queries/get-all-slaves :include-stale? true)]
      (is (= ["fresh"] (sort (map :slave/id default))))
      (is (= #{"fresh" "stale-row"} (set (map :slave/id all)))))))

(deftest get-slaves-by-status-test
  (testing "status filter returns only matching rows"
    (seed-slave! "a" {:status :idle})
    (seed-slave! "b" {:status :working})
    (seed-slave! "c" {:status :working})
    (is (= #{"b" "c"} (set (map :slave/id (queries/get-slaves-by-status :working)))))
    (is (= ["a"] (mapv :slave/id (queries/get-slaves-by-status :idle))))
    (is (empty? (queries/get-slaves-by-status :zombie)))))

(deftest get-slaves-by-project-test
  (testing "project filter, with the same stale gate as get-all-slaves"
    (seed-slave! "p1-a" {:project "proj-1"})
    (seed-slave! "p2-a" {:project "proj-2"})
    (seed-slave! "p1-stale" {:project "proj-1" :stale? true})
    (is (= ["p1-a"] (mapv :slave/id (queries/get-slaves-by-project "proj-1"))))
    (is (= #{"p1-a" "p1-stale"}
           (set (map :slave/id (queries/get-slaves-by-project "proj-1" :include-stale? true)))))
    (is (empty? (queries/get-slaves-by-project "missing")))))

(deftest get-slave-ids-by-project-test
  (testing "id-only projection for kill paths"
    (seed-slave! "p1-a" {:project "proj-1"})
    (seed-slave! "p2-a" {:project "proj-2"})
    (is (= #{"p1-a"} (set (queries/get-slave-ids-by-project "proj-1"))))))

(deftest get-slave-by-name-test
  (testing "name fallback lookup"
    (seed-slave! "swarm-abc-123" {:status :idle})
    (lings/update-slave! "swarm-abc-123" {})
    ;; add-slave! defaults :slave/name to the id; make a distinct name row:
    (let [c (conn/ensure-conn)
          eid (:db/id (d/entity @c [:slave/id "swarm-abc-123"]))]
      (d/transact! c [{:db/id eid :slave/name "github-connector"}]))
    (is (= "swarm-abc-123" (:slave/id (queries/get-slave-by-name "github-connector"))))
    (is (nil? (queries/get-slave-by-name "nobody")))))

(deftest get-slave-by-name-or-id-test
  (testing "exact id, exact name, 'agent:' stripping, then fuzzy keyword match"
    (seed-slave! "swarm-fix-wave-1770230187")
    (let [c (conn/ensure-conn)
          eid (:db/id (d/entity @c [:slave/id "swarm-fix-wave-1770230187"]))]
      (d/transact! c [{:db/id eid :slave/name "fix-wave"}]))
    ;; 1. exact id
    (is (= "swarm-fix-wave-1770230187"
           (:slave/id (queries/get-slave-by-name-or-id "swarm-fix-wave-1770230187"))))
    ;; 0. 'agent:' prefix stripped, then exact
    (is (= "swarm-fix-wave-1770230187"
           (:slave/id (queries/get-slave-by-name-or-id "agent:swarm-fix-wave-1770230187"))))
    ;; 2. exact name
    (is (= "swarm-fix-wave-1770230187"
           (:slave/id (queries/get-slave-by-name-or-id "fix-wave"))))
    ;; 3. fuzzy: keyword overlap (fix+wave) against the stored id
    (is (= "swarm-fix-wave-1770230187"
           (:slave/id (queries/get-slave-by-name-or-id "ling-fix-wave"))))
    (is (nil? (queries/get-slave-by-name-or-id "zzz-qqq-vvv")))))

(deftest get-slave-by-kanban-task-test
  (testing "kanban-task-id lookup"
    (seed-slave! "worker" {:kanban-task-id "kb-42"})
    (is (= "worker" (:slave/id (queries/get-slave-by-kanban-task "kb-42"))))
    (is (nil? (queries/get-slave-by-kanban-task "kb-99")))))

(deftest get-child-project-ids-test
  (testing "children of a project's slaves yield their distinct project ids"
    (seed-slave! "coord" {:project "pA"})
    (seed-slave! "kid1" {:project "pB" :parent "coord"})
    (seed-slave! "kid2" {:project "pC" :parent "coord"})
    (is (= #{"pB" "pC"} (queries/get-child-project-ids "pA")))
    (is (empty? (queries/get-child-project-ids "pZ")))))

;;; =============================================================================
;;; Task queries
;;; =============================================================================

(deftest get-tasks-for-slave-test
  (testing "tasks for a slave, with optional status filter"
    (seed-slave! "s-1")
    (seed-slave! "s-2")
    (seed-task! "t-1" "s-1" {:status :dispatched})
    (seed-task! "t-2" "s-1" {:status :completed})
    (seed-task! "t-3" "s-2" {:status :dispatched})
    (is (= #{"t-1" "t-2"} (set (map :task/id (queries/get-tasks-for-slave "s-1")))))
    (is (= ["t-1"] (mapv :task/id (queries/get-tasks-for-slave "s-1" :dispatched))))
    (is (= ["t-2"] (mapv :task/id (queries/get-tasks-for-slave "s-1" :completed))))
    (is (empty? (queries/get-tasks-for-slave "ghost")))
    (is (nil? (queries/get-task "no-such-task")))))

(deftest get-completed-tasks-test
  (testing "completed tasks sorted most-recent-first, filterable, limited"
    (seed-slave! "s-1")
    (seed-slave! "s-2")
    (seed-task! "t-1" "s-1")
    (seed-task! "t-2" "s-2")
    (seed-task! "t-3" "s-1")
    (lings/complete-task! "t-1")
    (Thread/sleep 5)
    (lings/complete-task! "t-2")
    (Thread/sleep 5)
    (lings/complete-task! "t-3")
    (let [all (queries/get-completed-tasks)]
      (is (= ["t-3" "t-2" "t-1"] (mapv :task/id all))))
    (is (= ["t-2"] (mapv :task/id
                         (queries/get-completed-tasks :slave-id "s-2"))))
    (is (= ["t-3" "t-2"] (mapv :task/id (queries/get-completed-tasks :limit 2))))
    (is (= ["t-3" "t-2" "t-1"]
           (mapv :task/id
                 (queries/get-completed-tasks
                  :since (java.util.Date. (- (System/currentTimeMillis) 60000))))))))

;;; =============================================================================
;;; Claim queries + conflicts
;;; =============================================================================

(deftest claim-queries-test
  (testing "get-claims-for-file / get-all-claims projections"
    (seed-slave! "s-1")
    (seed-slave! "s-2")
    (seed-task! "t-1" "s-1")
    ;; NOTE src bug: lings.clj:374 logs (subs prior-hash 0 8) which throws
    ;; StringIndexOutOfBoundsException when prior-hash is shorter than 8 chars.
    ;; Use a >=8-char hash here until the src guard is fixed.
    (lings/claim-file! "a.clj" "s-1" {:task-id "t-1" :prior-hash "h1hashhash"})
    (lings/claim-file! "b.clj" "s-2")
    (let [a (queries/get-claims-for-file "a.clj")]
      (is (= "s-1" (:slave-id a)))
      (is (= "t-1" (:task-id a)))
      ;; get-claims-for-file returns :claim/prior-hash verbatim (no truncation)
      (is (= "h1hashhash" (:prior-hash a))))
    (is (nil? (queries/get-claims-for-file "unclaimed.clj")))
    (let [all (queries/get-all-claims)]
      (is (= #{"a.clj" "b.clj"} (set (map :file all))))
      (is (= "s-2" (:slave-id (first (filter #(= "b.clj" (:file %)) all))))))))

(deftest has-conflict-test
  (testing "conflict is keyed on a DIFFERENT holder; same holder is no conflict"
    (seed-slave! "s-1")
    (seed-slave! "s-2")
    (lings/claim-file! "f.clj" "s-1")
    (is (true? (:conflict? (queries/has-conflict? "f.clj" "s-2"))))
    (is (= "s-1" (:held-by (queries/has-conflict? "f.clj" "s-2"))))
    (is (false? (:conflict? (queries/has-conflict? "f.clj" "s-1"))))
    (is (false? (:conflict? (queries/has-conflict? "other.clj" "s-2"))))))

(deftest check-file-conflicts-test
  (testing "batch conflict check lists only conflicting files"
    (seed-slave! "s-1")
    (seed-slave! "s-2")
    (lings/claim-file! "a.clj" "s-1")
    (lings/claim-file! "b.clj" "s-1")
    (is (= [{:file "a.clj" :held-by "s-1"} {:file "b.clj" :held-by "s-1"}]
           (sort-by :file (queries/check-file-conflicts "s-2" ["a.clj" "b.clj" "free.clj"]))))
    (is (empty? (queries/check-file-conflicts "s-1" ["a.clj"])))
    (is (empty? (queries/check-file-conflicts "s-2" [])))))

(deftest recent-claim-history-filters-test
  (testing "history filtered by slave-id"
    (seed-slave! "s-1")
    (seed-slave! "s-2")
    (lings/archive-claim-to-history! "x.clj" {:slave-id "s-1" :prior-hash "p"})
    (lings/archive-claim-to-history! "y.clj" {:slave-id "s-2" :prior-hash "q"})
    (let [hist (queries/get-recent-claim-history :slave-id "s-1")]
      (is (= ["x.clj"] (mapv :file hist))))
    (is (= 2 (count (queries/get-recent-claim-history))))))

;;; =============================================================================
;;; Stats + dump
;;; =============================================================================

(deftest db-stats-test
  (testing "db-stats counts reflect transacted state"
    (seed-slave! "s-1")
    (seed-slave! "s-2")
    (seed-task! "t-1" "s-1")
    (seed-task! "t-2" "s-1")
    (lings/claim-file! "a.clj" "s-1")
    (lings/complete-task! "t-1")
    (lings/add-to-wait-queue! "s-1" "a.clj")
    (lings/archive-claim-to-history! "a.clj" {:slave-id "s-1"})
    (let [stats (queries/db-stats)]
      (is (= 2 (:slaves stats)))
      (is (= 2 (:tasks stats)))
      (is (= 1 (:claims stats)))
      (is (= 1 (:active-tasks stats)) "t-2 still dispatched")
      ;; db-stats :completed-tasks counts :completed-task/* entities (written by
      ;; session-registry/register-completed-task!, not complete-task!) and
      ;; :wrap-queue counts :wrap-queue/* entities (written by wrap, not
      ;; add-to-wait-queue! which writes :wait-queue/*) — so both stay 0 here.
      (is (= 0 (:completed-tasks stats)))
      (is (= 0 (:wrap-queue stats)))
      (is (= 1 (:claim-history stats))))))

(deftest dump-db-test
  (testing "dump includes everything incl. stale slaves"
    (seed-slave! "s-1")
    (seed-slave! "z" {:stale? true})
    (let [dump (queries/dump-db)]
      (is (= #{"s-1" "z"} (set (map :slave/id (:slaves dump)))))
      (is (contains? dump :tasks))
      (is (contains? dump :claims))
      (is (contains? dump :stats)))))

;;; =============================================================================
;;; Property: status filter partitions the slave set (generative, no deps)
;;; =============================================================================

(def statuses [:idle :spawning :starting :initializing :working :blocked :error])

(deftest status-filter-partition-property-test
  "Property: for any generated status->count assignment, the union of
   per-status query results equals the full (include-stale?) set and the
   parts are pairwise disjoint."
  ;; The slave set ACCUMULATES across rounds on the shared test conn, so the
  ;; per-round union is only required to cover the slaves THIS round created
  ;; (earlier rounds' rows are already in `all`, and later rounds add more).
  []
  (dotimes [round 50]
    (let [;; generate: each round, a random subset of statuses each with
          ;; 1-3 slaves
          assignment (into {}
                           (for [s (shuffle statuses)
                                 :when (zero? (rand-int 2))]
                             [s (inc (rand-int 3))]))
          counter (atom 0)
          round-ids (atom [])]
      (doseq [[s n] assignment]
        (dotimes [_ n]
          (let [id (str "p" round "-" (swap! counter inc))]
            (swap! round-ids conj id)
            (seed-slave! id {:status s}))))
      (let [all (into #{}
                      (comp (map :slave/id)
                            (filter (set @round-ids)))
                      (queries/get-all-slaves :include-stale? true))
            parts (into {}
                        (map (fn [[s _]]
                               ;; get-slaves-by-status applies NO stale filter, so
                               ;; it also returns earlier rounds' rows sharing this
                               ;; status — filter to THIS round's ids, same as `all`.
                               [s (into #{}
                                        (comp (map :slave/id)
                                              (filter (set @round-ids)))
                                        (queries/get-slaves-by-status s))]))
                        assignment)
            union (apply clojure.set/union (vals parts))]
        (is (= all union) (str "union mismatch @" round))
        ;; pairwise disjoint: any two distinct statuses share no id
        (let [pairs (for [x assignment y assignment :when (not= (key x) (key y))] [(key x) (key y)])]
          (doseq [[s1 s2] pairs]
            (is (empty? (clojure.set/intersection (parts s1) (parts s2)))
                (str "overlap " s1 "/" s2 " @" round))))))))
