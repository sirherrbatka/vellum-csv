(cl:in-package #:vellum-csv)


(defun csv-range (path/range)
  (make-instance
   'csv-range :original-range (if (or (stringp path/range)
                                      (pathnamep path/range))
                                  (cl-ds.fs:line-by-line path/range)
                                  path/range)))
