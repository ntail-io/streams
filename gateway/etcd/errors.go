package etcd

import "errors"

var ErrMissingLease = errors.New("missing lease on key")
var ErrInconsistentState = errors.New("domain state is inconsistent to etcd")
