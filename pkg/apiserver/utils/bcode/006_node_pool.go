package bcode

var ErrNodePoolInvalid = NewBcode(400, 35000, "node pool is invalid")
var ErrNodePoolExists = NewBcode(409, 35001, "node pool already exists")
var ErrNodePoolNotFound = NewBcode(404, 35002, "node pool not found")
