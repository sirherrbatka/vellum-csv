(cl:in-package #:vellum-csv)


(defun csv-range (path/range &key includes-header-p)
  (make-instance
   'csv-range :original-range (if (or (stringp path/range)
                                      (pathnamep path/range))
                                  (let ((line-by-line (cl-ds.fs:line-by-line path/range)))
                                    (if includes-header-p
                                        (progn (cl-ds:consume-front line-by-line)
                                               (cl-ds:clone line-by-line))
                                        line-by-line))
                                  path/range)
              :includes-header-p includes-header-p))
