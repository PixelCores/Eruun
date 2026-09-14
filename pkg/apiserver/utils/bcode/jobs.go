package bcode

var ErrJobInput = NewBcode(400, 34000, "invalid Job request")
var ErrJobResultExpired = NewBcode(410, 34001, "original Job result has expired")
var ErrJobResultConflict = NewBcode(409, 34002, "Job result is immutable")
var ErrJobTooLarge = NewBcode(413, 34003, "Job archive exceeds its size limit")
var ErrJobRunnerConflict = NewBcode(409, 34004, "Job runner state conflicts with the active execution")
