(cl:in-package #:vellum-csv)


(defclass csv-range (vellum.header:frame-range-mixin
                     cl-ds.alg:forward-proxy-range)
  ((%includes-header-p :initarg :includes-header-p
                       :reader includes-header-p)
   (%separator :initarg :separator
               :reader separator))
  (:default-initargs
   :separator #\,))


(defmethod cl-ds.utils:cloning-information append ((object csv-range))
  '((:includes-header-p includes-header-p)))
