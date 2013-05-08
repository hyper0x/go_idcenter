package base

import (
	"go_lib/logging"
)

// base
const (
	CONFIG_FILE_NAME = "id_center.config"
)

var logger logging.Logger = logging.GetSimpleLogger()

func Logger() logging.Logger {
	return logger
}
