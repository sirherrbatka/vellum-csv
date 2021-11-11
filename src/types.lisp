(cl:in-package #:vellum-csv)


(defclass csv-range (vellum.header:frame-range-mixin
                     cl-ds:traversable)
  ((%includes-header-p :initarg :includes-header-p
                       :reader includes-header-p)
   (%path :initarg :path
          :reader path)
   (%separator :initarg :separator
               :reader separator)
   (%quote :initarg :quote
           :reader csv-quote)
   (%escape :initarg :escape
            :reader escape)))


(defmethod cl-ds.utils:cloning-information append ((object csv-range))
  '((:includes-header-p includes-header-p)
    (:escape escape)
    (:separator separator)
    (:path path)
    (:quote csv-quote)))
