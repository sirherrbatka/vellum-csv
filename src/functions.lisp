(cl:in-package #:vellum-csv)


(defun csv-range (path/range &key includes-header-p)
  (make-instance
   'csv-range :original-range (if (or (stringp path/range)
                                      (pathnamep path/range))
                                  (~> (cl-ds.fs:line-by-line path/range)
                                      (cl-ds:drop-front (if includes-header-p 1 0))
                                      cl-ds:clone)
                                  path/range)
              :header includes-header-p))
