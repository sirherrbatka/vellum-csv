(cl:in-package #:vellum-csv)

(prove:plan 17)

(let ((frame
        (vellum:copy-from
         :csv
         (asdf:system-relative-pathname :vellum-csv
                                        "test.csv")
         :includes-header-p t
         :columns '(map-name
                    (:name fire-pitch :type single-float)
                    (:name fire-yaw :type single-float)
                    (:name fire-x :type single-float)
                    (:name fire-y :type single-float)
                    (:name fire-z :type single-float)
                    (:name target-x :type single-float)
                    (:name target-y :type single-float)
                    (:name target-z :type single-float))
         :body (vellum:bind-row (map-name
                                 fire-pitch fire-yaw fire-x fire-y fire-z
                                 target-x target-y target-z)
                 (unless (string= map-name "de_inferno")
                   (vellum:drop-row))
                 (when (some (curry #'eq :null)
                             (list fire-pitch fire-yaw fire-x fire-y fire-z target-x target-y target-z))
                   (vellum:drop-row))))))
  (prove:is (vellum:row-count frame) 6))

(let ((list (first (csv-to-list "1,2,3" #\, #\" #\"))))
  (prove:is (length list) 3)
  (prove:is list '("1" "2" "3")))

(let ((list (first (csv-to-list "1  , 2,3" #\, #\" #\"))))
  (prove:is (length list) 3)
  (prove:is list '("1" "2" "3")))

(let ((list (first (csv-to-list "1 ioen , 2,3" #\, #\" #\"))))
  (prove:is (length list) 3)
  (prove:is list '("1 ioen" "2" "3")))

(let ((list (first (csv-to-list "  1 ioen , 2,3" #\, #\" #\"))))
  (prove:is (length list) 3)
  (prove:is list '("1 ioen" "2" "3")))

(let ((list (first (csv-to-list "  1 i oen , 2,3" #\, #\" #\"))))
  (prove:is (length list) 3)
  (prove:is list '("1 i oen" "2" "3")))

(let ((list (first (csv-to-list ", ," #\, #\" #\"))))
  (prove:is (length list) 3)
  (prove:is list '("" "" "")))

(let ((list (first (csv-to-list ", ,    5   " #\, #\" #\"))))
  (prove:is (length list) 3)
  (prove:is list '("" "" "5")))

(let ((list (csv-to-list "1,2,3

                          4,5,6"
                         #\, #\" #\")))
  (prove:is list '(("1" "2" "3") ("4" "5" "6"))))

(let ((list (csv-to-list "1,2,3

                          4,5,6
"
                         #\, #\" #\")))
  (prove:is list '(("1" "2" "3") ("4" "5" "6"))))

(prove:finalize)
