(cl:in-package #:vellum-csv)


(defun csv-range (path &key includes-header-p header separator quote line-endings
                         unquoted-quotequote)
  (make-instance
   'csv-range :path path
              :header header
              :line-endings line-endings
              :unquoted-quotequote unquoted-quotequote
              :quote quote
              :separator separator
              :includes-header-p includes-header-p))
