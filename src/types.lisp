(cl:in-package #:vellum-csv)


(defclass csv-range (vellum.header:frame-range-mixin
                     cl-ds:traversable)
  ((%includes-header-p :initarg :includes-header-p
                       :reader includes-header-p)
   (%unquoted-quotequote :initarg :unquoted-quotequote
                         :reader unquoted-quotequote)
   (%path :initarg :path
          :reader path)
   (%separator :initarg :separator
               :reader separator)
   (%skip-whitespace :initarg :skip-whitespace
                     :reader skip-whitespace)
   (%quote :initarg :quote
           :reader csv-quote)
   (%line-endings :initarg :line-endings
                  :reader line-endings))
  (:default-initargs
   :separator #\,
   :skip-whitespace nil
   :line-endings (list +cr+ +lf+ +crlf+)
   :unquoted-quotequote nil
   :quote #\"))


(defmethod cl-ds.utils:cloning-information append ((object csv-range))
  '((:includes-header-p includes-header-p)
    (:separator separator)
    (:line-endings line-endings)
    (:quote csv-quote)))
