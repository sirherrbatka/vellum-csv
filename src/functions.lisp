(cl:in-package #:vellum-csv)


(defun csv-range (path &key includes-header-p header separator quote )
  (check-type path (or string pathname cl-ds.fs:command))
  (check-type header vellum.header:standard-header)
  (check-type separator character)
  (check-type quote character)
  (make-instance
   'csv-range :path path
              :header header
              :quote quote
              :separator separator
              :includes-header-p includes-header-p))
