(cl:in-package #:vellum-csv)


(defclass csv-range (cl-ds.alg:proxy-range)
  ((%header :initarg :includes-header-p
            :reader includes-header-p)))


(defmethod cl-ds.utils:cloning-information append ((object csv-range))
  '((:includes-header-p includes-header-p)))
