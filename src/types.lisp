(cl:in-package #:vellum-csv)


(defclass csv-range (cl-ds.alg:proxy-range)
  ((%header :initarg :header
            :reader header)))


(defmethod cl-ds.utils:cloning-information append ((object csv-range))
  '((:header header)))
