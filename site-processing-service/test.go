package main

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/net/html"
)

func testProcessHtml() {
	file, _ := os.Open("./test/MucLuc_ThoGaiGoc.html")
	ioreader := io.Reader(file)
	siteLink := SiteLink{Host: "thophuongha.com", Path: "/MucLuc_ThoGaiGoc.html"}
	res := processHtml(html.NewTokenizer(ioreader), &siteLink, nil)
	fmt.Println(res)
}
