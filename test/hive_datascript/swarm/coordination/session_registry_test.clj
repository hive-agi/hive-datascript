(ns hive-datascript.swarm.coordination.session-registry-test
  "Tests for hive-datascript.swarm.coordination.session-registry.
   Fresh in-memory conn per test via connection/*test-conn*."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.hooks :as hooks]
            [hive-datascript.swarm.coordination.session-registry :as sr]))

(defn fresh-conn-fixture [f]
  (hooks/clear!)
  (conn/with-test-conn (conn/create-conn) f))

(use-fixtures :each fresh-conn-fixture)

;;; =============================================================================
;;; Completed tasks
;;; =============================================================================

(deftest register-and-get-completed-task-test
  (testing "register stores title/agent/project/session + timestamp"
    (sr/register-completed-task! "t-1"
                                 {:title "Fix bug"
                                  :agent-id "agent-a"
                                  :project-id "proj-x"
                                  :session-id "sess-1"})
    (let [row (sr/get-completed-task "t-1")]
      (is (some? row))
      (is (= "t-1" (:completed-task/id row)))
      (is (= "Fix bug" (:completed-task/title row)))
      (is (= "agent-a" (:completed-task/agent-id row)))
      (is (= "proj-x" (:completed-task/project-id row)))
      (is (= "sess-1" (:completed-task/session-id row)))
      (is (some? (:completed-task/completed-at row))))
    (is (nil? (sr/get-completed-task "missing")))))

(deftest register-completed-task-validation-test
  (testing "task-id must be a string"
    ;; src throws AssertionError (an Error, not an Exception)
    (is (thrown? AssertionError (sr/register-completed-task! 123 {})))))

(deftest get-completed-tasks-ordering-test
  (testing "most recent completion first"
    (sr/register-completed-task! "t-1" {})
    (Thread/sleep 5)
    (sr/register-completed-task! "t-2" {})
    (Thread/sleep 5)
    (sr/register-completed-task! "t-3" {})
    (is (= ["t-3" "t-2" "t-1"]
           (mapv :completed-task/id (sr/get-completed-tasks-this-session))))))

(deftest get-completed-tasks-filters-test
  (testing "agent-id and project-id filters"
    (sr/register-completed-task! "t-1" {:agent-id "a1" :project-id "p1"})
    (sr/register-completed-task! "t-2" {:agent-id "a2" :project-id "p2"})
    (is (= ["t-1"] (mapv :completed-task/id
                         (sr/get-completed-tasks-this-session :agent-id "a1"))))
    (is (= ["t-2"] (mapv :completed-task/id
                         (sr/get-completed-tasks-this-session :project-id "p2"))))
    (is (empty? (sr/get-completed-tasks-this-session :agent-id "zzz")))))

(defn- ancestors-of
  "Walk the parent-of map from SESSION-ID upward."
  [parent-of session-id]
  (loop [cur (get parent-of session-id)
         acc #{}]
    (if (or (nil? cur) (contains? acc cur))
      acc
      (recur (get parent-of cur) (conj acc cur)))))

(deftest session-scoping-test
  (testing "a :scope-rows hook + :session-ref restricts reads to owned rows"
    (hooks/install! {:scope-rows
                     (fn [{:keys [session-ref parent-of]} session-key rows]
                       (if (nil? session-ref)
                         rows
                         (filter (fn [row]
                                   (let [sid (get row session-key)]
                                     (or (= sid session-ref)
                                         ;; owner walk: session-ref is an
                                         ;; ancestor of the row's session
                                         (some #(= session-ref %)
                                               (ancestors-of parent-of sid)))))
                                 rows)))})
    (sr/register-completed-task! "own"   {:session-id "me"})
    (sr/register-completed-task! "child" {:session-id "me:1"})
    (sr/register-completed-task! "other" {:session-id "other"})
    (let [rows (sr/get-completed-tasks-this-session :session-ref "me" :parent-of {"me:1" "me"})]
      (is (= #{"own" "child"} (set (map :completed-task/id rows)))))
    ;; unscoped read sees everything
    (is (= 3 (count (sr/get-completed-tasks-this-session))))))

(deftest clear-completed-tasks-test
  (testing "1-arity clears only the harvested ids; 0-arity resets the store"
    (sr/register-completed-task! "t-1" {})
    (sr/register-completed-task! "t-2" {})
    (sr/register-completed-task! "t-3" {})
    (is (= 2 (sr/clear-completed-tasks! ["t-1" "t-2" nil])))
    (is (= ["t-3"] (mapv :completed-task/id (sr/get-completed-tasks-this-session))))
    (is (= 0 (sr/clear-completed-tasks! ["already-gone"])))
    (is (= 1 (sr/clear-completed-tasks!)))
    (is (empty? (sr/get-completed-tasks-this-session)))))

;;; =============================================================================
;;; Kanban movements
;;; =============================================================================

(deftest register-and-get-kanban-movement-test
  (testing "movement rows record from/to/task/title"
    (sr/register-kanban-movement! {:task-id "k-1" :from nil :to "todo"
                                   :title "born" :session-id "s1"})
    (Thread/sleep 2)
    (sr/register-kanban-movement! {:task-id "k-1" :to "done"})
    (let [mvs (sr/get-kanban-movements-this-session)]
      (is (= 2 (count mvs)))
      (is (= "born" (:kanban-movement/title (first mvs))))))
  (testing "1-arity call + chronological ordering + filters"
    (sr/register-kanban-movement! {:task-id "k-2" :from "todo" :to "doing"
                                   :project-id "p1" :agent-id "a1" :session-id "s1"})
    (Thread/sleep 2)
    (sr/register-kanban-movement! {:task-id "k-2" :from "doing" :to "done"
                                   :project-id "p1" :agent-id "a2" :session-id "s2"})
    (let [mvs (->> (sr/get-kanban-movements-this-session)
                   (filter #(= "k-2" (:kanban-movement/task-id %))))]
      (is (= ["todo" "doing"] [(-> mvs first :kanban-movement/from)
                             (-> mvs first :kanban-movement/to)])
          "chronological order (oldest first)")
      (is (= 2 (count mvs))))
    (is (= 1 (count (sr/get-kanban-movements-this-session :agent-id "a1"))))
    (is (= 2 (count (sr/get-kanban-movements-this-session :project-id "p1"))))
    (is (empty? (sr/get-kanban-movements-this-session :agent-id "zzz")))))

(deftest register-movement-validation-test
  (testing "task-id and :to must be strings"
    ;; src :pre throws AssertionError (task-id and :to must be strings)
    (is (thrown? AssertionError (sr/register-kanban-movement! {:task-id 5 :to "done"})))
    (is (thrown? AssertionError (sr/register-kanban-movement! {:task-id "k-1"})))))

(deftest clear-kanban-movements-test
  (testing "1-arity clears only harvested ids; 0-arity resets"
    (sr/register-kanban-movement! {:task-id "k-1" :to "todo"})
    (sr/register-kanban-movement! {:task-id "k-2" :to "doing"})
    (let [ids (mapv :kanban-movement/id (sr/get-kanban-movements-this-session))]
      (is (= 1 (sr/clear-kanban-movements! [(first ids)]))))
    (is (= 1 (count (sr/get-kanban-movements-this-session))))
    (is (= 1 (sr/clear-kanban-movements!)))
    (is (empty? (sr/get-kanban-movements-this-session)))))

(deftest session-scoping-movements-test
  (testing "scoped read of movements via :scope-rows hook"
    (hooks/install! {:scope-rows
                     (fn [{:keys [session-ref]} session-key rows]
                       (if (nil? session-ref)
                         rows
                         (filter #(= session-ref (get % session-key)) rows)))})
    (sr/register-kanban-movement! {:task-id "k-1" :to "todo" :session-id "me"})
    (sr/register-kanban-movement! {:task-id "k-2" :to "todo" :session-id "you"})
    (is (= ["k-1"] (mapv :kanban-movement/task-id
                         (sr/get-kanban-movements-this-session :session-ref "me"))))))

;;; =============================================================================
;;; Property: register/get/clear round-trips for generated ids
;;; =============================================================================

(deftest completed-task-roundtrip-property-test
  "Property: any generated (task-id, session-id) pair registered is
   retrievable until explicitly cleared, and clearing its id removes
   exactly that row."
  []
  (dotimes [i 150]
    (let [id (str "pt-" i)
          sess (str "sess-" (rand-int 3))]
      (sr/register-completed-task! id {:session-id sess :title (str "T" i)})
      (let [row (sr/get-completed-task id)]
        (is (some? row) (str "missing @" i))
        (is (= sess (:completed-task/session-id row)))
        (is (= (str "T" i) (:completed-task/title row))))
      (is (= 1 (sr/clear-completed-tasks! [id])))
      (is (nil? (sr/get-completed-task id)) (str "still there @" i)))))
