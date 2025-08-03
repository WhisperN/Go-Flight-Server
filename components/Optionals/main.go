package Optionals

// This file contains only optionals

type ADDRESS struct {
	IP   *string
	PORT *string //
}

func (a *ADDRESS) Check() bool {
	if a.IP == nil {
		*a.IP = "0.0.0.0"
	}
	if a.PORT == nil {
		*a.PORT = "8080"
	}
	return true
}

func String(input string) *string {
	return &input
}

func main() {

}
