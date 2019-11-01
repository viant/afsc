package logger

import (
	"log"
	"strings"
)

//Log represent log function
type Log func(format string, args ...interface{})

//Logf - function to log debug info
var Logf Log = VoidLogger

//VoidLogger represent logger that do not log
func VoidLogger(format string, args ...interface{}) {

}

//StdoutLogger represents stdout logger
func StdoutLogger(format string, args ...interface{}) {
	if !strings.Contains(format, "\n") {
		format += "\n"
	}
	log.Printf(format, args...)
}
