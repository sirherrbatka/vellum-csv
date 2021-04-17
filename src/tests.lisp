(cl:in-package #:vellum-csv)

(prove:plan 1)

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

(prove:finalize 1)
