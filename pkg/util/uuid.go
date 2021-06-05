package util

import (
	"fmt"
	"github.com/google/uuid"
)

type UID string

func NewUUID() UID {
	return UID(uuid.New().String())
}

func NewUUIDString() string {
	return fmt.Sprintf("%v", NewUUID())
}
