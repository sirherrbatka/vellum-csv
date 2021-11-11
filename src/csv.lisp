(in-package :vellum-csv)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (define-constant +cr+ #.(format nil "~A" #\Return)
    :test 'equal)
  (define-constant +lf+ #.(format nil "~A" #\Linefeed)
    :test 'equal)
  (define-constant +crlf+ #.(format nil "~A~A" #\Return #\Linefeed)
    :test 'equal)
  (defparameter *csv-variables* '())) ; list of (var rfc4180-value creativyst-value)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (macrolet
      ((define (var rfc4180 creativyst doc)
         `(progn
            (eval-when (:compile-toplevel :load-toplevel :execute)
              (pushnew `(,',var ,,rfc4180 ,,creativyst) *csv-variables* :key #'car))
            (defparameter ,var ,creativyst ,doc))))
    (define *separator*
  #\, #\,
      "Separator between CSV fields")
    (define *quote*
  #\" #\"
      "delimiter of string data; pascal-like quoted as double itself in a string.")
    (define *unquoted-quotequote*
  nil nil
      "does a pair of quotes represent a quote outside of quotes?
M$, RFC says NIL, csv.3tcl says T")
    (define *loose-quote*
  nil nil
      "can quotes appear anywhere in a field?")
    (define *allow-binary*
  t t
      "do we accept non-ascii data?")
    (define *keep-meta-info*
  nil nil
      "when parsing, include meta information?")
    (define *skip-whitespace*
  nil t
      "shall we skip unquoted whitespace around separators?")
    (define *line-endings*
      (list +crlf+ +lf+) (list +cr+ +lf+ +crlf+)
      "acceptable line endings when importing CSV")
    (define *eol*
      +lf+ +crlf+
      "line ending when exporting CSV")
    ))

(defun char-ascii-text-p (c)
  (<= #x20 (char-code c) #x7E))

(defun valid-eol-p (x)
  (member x (list +cr+ +lf+ +crlf+) :test #'equal))

(defun validate-csv-parameters ()
  (assert (typep *separator* 'character) ())
  (assert (typep *quote* 'character) ())
  (assert (not (eql *separator* *quote*)) ())
  (assert (typep *unquoted-quotequote* 'boolean) ())
  (assert (typep *loose-quote* 'boolean) ())
  (assert (valid-eol-p *eol*) ())
  (assert (and *line-endings* (every #'valid-eol-p *line-endings*)) ())
  (assert (typep *keep-meta-info* 'boolean) ())
  (assert (typep *skip-whitespace* 'boolean) ()))

(defvar *accept-cr* t "internal: do we accept cr?")
(defvar *accept-lf* t "internal: do we accept lf?")
(defvar *accept-crlf* t "internal: do we accept crlf?")

(define-constant +buffer-size+ 4096)

; -----------------------------------------------------------------------------

(declaim (inline char-space-p))
(defun char-space-p (separator c)
  "Is character C some kind of white space?
NB: this only handles a tiny subset of whitespace characters,
even if restricted to ASCII. However, it's rather portable,
and is what the creativyst document specifies.
Be careful to not skip a separator, as it could be e.g. a tab!"
  (declare (type (or null character) c)
           (type character separator)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  (and c
       (or (eql c #\Space)
           (eql c #\Tab))
       (not (eql c separator))))


(defmacro accept (x stream &optional (ensured nil))
  (once-only (x stream)
    `(let ((c (buffered-stream-peek ,stream ,ensured)))
       (if (etypecase ,x
              (character (eql ,x c))
              ((or function symbol) (funcall ,x c))
              (fixnum (= ,x (char-code c))))
           (progn
             (incf (buffered-stream-stream-position ,stream))
             c)
           nil))))


(declaim (inline accept-space))
(defun accept-space (stream)
  (declare (type buffered-stream stream)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  (let ((separator (buffered-stream-separator stream)))
    (accept (lambda (x) (char-space-p separator x)) stream)))


(declaim (inline accept-spaces))
(defun accept-spaces (stream)
  (iterate
    (for x = (accept-space stream))
    (while x)
    (collect x)))


(defmacro accept-eof (stream &optional (ensured nil))
  `(not (buffered-stream-peek ,stream ,ensured)))

;; ---------------------------------------------------------------------------
;;     Title: A very simple CSV Reader
;;   Created: 2021-11-11
;;    Author: Gilbert Baumann
;;   License: MIT style (see below)
;; ---------------------------------------------------------------------------
;;;  (c) copyright 2021 by Gilbert Baumann

;;  Permission is hereby granted, free of charge, to any person obtaining
;;  a copy of this software and associated documentation files (the
;;  "Software"), to deal in the Software without restriction, including
;;  without limitation the rights to use, copy, modify, merge, publish,
;;  distribute, sublicense, and/or sell copies of the Software, and to
;;  permit persons to whom the Software is furnished to do so, subject to
;;  the following conditions:
;;
;;  The above copyright notice and this permission notice shall be
;;  included in all copies or substantial portions of the Software.
;;
;;  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
;;  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
;;  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
;;  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
;;  CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
;;  TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
;;  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
(declaim (inline read-csv))
(defun read-csv (stream row-callback column-callback
                 separator quote)
  (declare (type character separator quote))
  (let* ((minimum-room 4096)
         (row-callback (ensure-function row-callback))
         (column-callback (ensure-function column-callback))
         (buffer (make-array (* 2 minimum-room) :element-type 'character))
         (start 0)                      ;start of our current field
         ;; fill pointer of buffer
         (fptr 0)
         (p -1)                         ;reading pointer
         (c #\space))                   ;lookahead
    (declare (type (simple-array character (*)) buffer)
             (type fixnum start fptr p minimum-room)
             (optimize (speed 3) (safety 0)))
    (labels ((underflow ()
               (replace buffer buffer :start2 start :end2 p)
               (decf p start)
               (decf start start)
               (cond ((stringp stream)
                      (setq fptr p))
                     (t
                      (when (<= (- (length buffer) start) minimum-room)
                        (setq buffer (adjust-array buffer (+ (length buffer) minimum-room)))
                        (setq minimum-room (* minimum-room 2)))
                      (setf fptr (read-sequence buffer stream :start p)))))
             (consume ()
               (when c
                 (incf p)
                 (when (= p fptr) (underflow)))
               (setq c (if (= p fptr) nil (char buffer p))))
             (read-field ()
               (cond ((eql quote c)
                      (consume)
                      (read-dquote-field))
                     (t
                      (setq start p)
                      (loop until (member c `(,separator #\newline nil)) do (consume))
                      (funcall column-callback buffer start p))))
             (read-dquote-field ()
               (let ((value (format nil "~{~A~^\"~}"
                                    (loop collect (progn
                                                    (setq start p)
                                                    (loop until (member c `(,quote nil)) do (consume))
                                                    (subseq buffer start p))
                                          do (when (eql quote c) (consume))
                                          while (eql quote c) do (consume)))))
                   (funcall column-callback
                            value
                            0
                            (length value))
                 (assert (member c (list #\newline separator nil)))))
             (read-row ()
               ;; Question: What is an empty line? One empty field, or no field?
               (prog1
                   (loop collect (read-field) while (eql c separator) do (consume))
                 (ecase c ((#\newline nil)))
                 (consume))))
      (declare (inline underflow consume read-field read-row))
      (consume)
      (loop until (null c) :do (progn
                                 (read-row)
                                 (funcall row-callback))))))


(defun char-needs-quoting (x)
  (or (eql x *quote*)
      (eql x *separator*)
      (not (char-ascii-text-p x))))

(defun string-needs-quoting (x)
  (and (not (zerop (length x)))
       (or (char-space-p *separator* (char x 0))
     (char-space-p *separator* (char x (1- (length x))))
     (some #'char-needs-quoting x))
       t))

(defun write-csv-lines (lines stream)
  "Given a list of LINES, each of them a list of fields, and a STREAM,
  format those lines as CSV according to the current syntax parameters."
  (dolist (x lines)
    (write-csv-line x stream)))

(defun write-csv-line (fields stream)
  "Format one line of FIELDS to STREAM in CSV format,
  using the current syntax parameters."
  (loop :for x :on fields :do
    (write-csv-field (first x) stream)
    (when (cdr x)
      (write-char *separator* stream)))
  (write-string *eol* stream))

(defun write-csv-field (field stream)
  (etypecase field
    (null t)
    (number (princ field stream))
    (string (write-csv-string-safely field stream))
    (symbol (write-csv-string-safely (symbol-name field) stream))))

(defun write-csv-string-safely (string stream)
  (if (string-needs-quoting string)
      (write-quoted-string string stream)
      (write-string string stream)))

(defun write-quoted-string (string stream)
  (write-char *quote* stream)
  (loop :for c :across string :do
    (when (char= c *quote*)
      (write-char c stream))
    (write-char c stream))
  (write-char *quote* stream))
