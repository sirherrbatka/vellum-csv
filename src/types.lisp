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


(define-condition could-not-parse (error)
  ((%buffer :initarg :buffer
            :reader buffer)
   (%start :initarg :start
           :reader start)
   (%end :initarg :end
         :reader end)
   (%column-index :initarg :column-index
                  :reader column-index)
   (%column-type :initarg :column-type
                 :reader column-type)
   (%original-error :reader original-error
                    :initarg :original-error))
  (:report (lambda (object stream)
             (with-accessors ((buffer buffer)
                              (start start)
                              (end end)
                              (column-index column-index)
                              (column-type column-type)
                              (original-error original-error))
                 object
               (format stream "Could not parse ~a for colum ~a of type ~a.~%Reason: ~a"
                       (subseq buffer start end)
                       column-index
                       column-type
                       original-error)))))


(defmethod cl-ds.utils:cloning-information append ((object csv-range))
  '((:includes-header-p includes-header-p)
    (:escape escape)
    (:separator separator)
    (:path path)
    (:quote csv-quote)))

