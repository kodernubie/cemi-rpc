package main

import (
	"fmt"

	_ "github.com/kodernubie/cemi-rpc/example/advance/server/svc/usersvc"
)

func main() {

	fmt.Println("Server started")

	select {}
}
