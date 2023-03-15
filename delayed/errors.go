package delayed

import "errors"

var InvalidHandlerError = errors.New("Invalid handler")
var InvalidTaskError = errors.New("Invalid task")
var InvalidRedisReplyError = errors.New("Invalid redis reply")
