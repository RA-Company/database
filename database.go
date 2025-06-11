package database

import (
	"fmt"
)

var (
	ErrorIncorrectParameters = fmt.Errorf("incorrect parameters")
	ErrorIncorrectRequest    = fmt.Errorf("incorrect request")
	ErrorDatabaseError       = fmt.Errorf("database error")
	ErrorNotFound            = fmt.Errorf("not found")
	ErrorIncorrectID         = fmt.Errorf("incorrect ID")
)
