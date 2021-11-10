(cl:in-package #:vellum-csv)


(defun csv-range (path &key includes-header-p header separator quote)
  (make-instance
   'csv-range :path path
              :header header
              :quote quote
              :separator separator
              :includes-header-p includes-header-p))
