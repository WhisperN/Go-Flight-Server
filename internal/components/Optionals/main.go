package Optionals

// This file contains only optionals

type ADDRESS struct {
	IP   *string
	PORT *string //
}

func (a *ADDRESS) Check() bool {
	if a.IP == nil {
		return false
	}
	if a.PORT == nil {
		return false
	}
	return true
}

func String(input string) *string {
	return &input
}
