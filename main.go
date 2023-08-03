package main

import (
	"flag"
	"github.com/Pangjiping/copier/copier"
	"log"
	"time"

	cp "github.com/otiai10/copy"
)

var (
	worker  uint
	srcDir  string
	destDir string
)

func main() {
	flag.StringVar(&srcDir, "src", "", "the src directory")
	flag.StringVar(&destDir, "dest", "", "the dest directory")
	flag.UintVar(&worker, "worker", 0, "the worker number")
	flag.Parse()

	startTime := time.Now()
	err := copier.NewCopier(srcDir, destDir, worker, cp.Options{
		OnDirExists: func(src, dest string) cp.DirExistsAction {
			return cp.Replace
		},
		OnSymlink: func(src string) cp.SymlinkAction {
			log.Printf("processing symlink %q ...\n", src)
			if src == srcDir {
				return cp.Deep
			}
			// for the symbolic link inside the seed_dir, use shallow copy
			return cp.Shallow
		},
		PermissionControl: cp.DoNothing,
		Skip:              nil,
	}).Copy()
	if err != nil {
		log.Panic(err)
	}

	log.Printf("copier finished in %s.\n", time.Since(startTime).String())
}
