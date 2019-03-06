//+build mage

// Run using mage: https://github.com/magefile/mage
//
// Example invocation:
//   mage cover
//
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/shabbyrobe/gocovmerge"
	"golang.org/x/tools/cover"
)

// Cover collects true code coverage for all packages in gofakes3.
// It does so by running 'go test' for each child package (enumerated by
// 'go list ./...') with the '-coverpkg' flag, populated with the same
// 'go list'.
func Cover() {
	pkgs := goList()

	var files []string

	for _, pkg := range pkgs {
		covFile, err := ioutil.TempFile("", "")
		if err != nil {
			panic(err)
		}
		covFile.Close()
		defer os.Remove(covFile.Name())

		files = append(files, covFile.Name())
		cmd := exec.Command("go", "test",
			fmt.Sprintf("-coverprofile=%s", covFile.Name()),
			fmt.Sprintf("-coverpkg=%s", strings.Join(pkgs, ",")),
			pkg,
		)
		if err := cmd.Run(); err != nil {
			panic(err)
		}
	}

	var merged []*cover.Profile
	for _, file := range files {
		profiles, err := cover.ParseProfiles(file)
		if err != nil {
			panic(fmt.Errorf("failed to parse profiles: %v", err))
		}
		for _, p := range profiles {
			merged = gocovmerge.AddProfile(merged, p)
		}
	}

	gocovmerge.DumpProfiles(merged, os.Stdout)
}

func goList() (pkgs []string) {
	cmd := exec.Command("go", "list", "./...")

	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}
		pkgs = append(pkgs, line)
	}
	return pkgs
}