package copier

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"sync"

	cp "github.com/otiai10/copy"
)

const (
	defaultWorkerNumber uint = 8
)

// Copier is a parallel copy util which is compatible with the underlying capability of cp util.
// More about cp util: https://github.com/otiai10/copy
//
// This implementation has the following features:
//
// 1. Multi goroutine workers are used to parse nested directory tree and dispatch tasks through channel.
//
// 2. Copier will parse directory layer by layer until it reaches a single-layer directory, file or symlink, then hand that over to the cp util for real copy.
//
// 3. If srcDir is symlink, Copier will parse the symlink and modify srcDir to real directory or file.
//
// 4. Please set worker reasonably. When every file is large, please use a smaller worker. For m2_repo, you could use more workers to accelerate copy.
type Copier struct {
	worker  uint
	srcDir  string
	destDir string
	// TODO: 有个bug，option用了外部变量，会导致递归之间互相影响
	cpOptions cp.Options

	dch chan string
	wch chan string
	wg  sync.WaitGroup
}

func NewCopier(srcDir, destDir string, worker uint, options ...cp.Options) *Copier {
	workerNum := defaultWorkerNumber
	if worker > 0 {
		workerNum = worker
	}
	log.Printf("[Copier] worker number is %v\n", workerNum)

	opt := cp.Options{}
	if len(options) > 0 {
		opt = options[0]
	}

	return &Copier{
		worker:    workerNum,
		srcDir:    srcDir,
		destDir:   destDir,
		dch:       make(chan string, 100),
		wch:       make(chan string, 100),
		wg:        sync.WaitGroup{},
		cpOptions: opt,
	}
}

func (c *Copier) Copy() error {
	handover, err := c.preStart()
	if err != nil {
		return err
	}
	if handover {
		return cp.Copy(c.srcDir, c.destDir, c.cpOptions)
	}

	go c.dispatcher()
	for i := uint(0); i < c.worker; i++ {
		go c.copy(i)
	}

	c.wg.Add(1)
	c.dch <- ""

	c.wg.Wait()
	return nil
}

func (c *Copier) preStart() (bool, error) {
	_, err := os.Stat(c.destDir)
	if os.IsNotExist(err) {
		if err = MkdirWithFullPermission(c.destDir); err != nil {
			return false, fmt.Errorf("[Copier] failed to create destination directory %s, error: %w", c.destDir, err)
		}
	}
	if err != nil {
		return false, fmt.Errorf("[Copier] failed to stat destination directory %s, error: %w", c.destDir, err)
	}

	src, err := os.Lstat(c.srcDir)
	if src.Mode()&os.ModeSymlink == 0 && src.IsDir() {
		return false, nil
	}

	// If the top level is symlink, find behind real directory
	if src.Mode()&os.ModeSymlink == os.ModeSymlink {
		if err := c.readSrcSymlink(); err != nil {
			return false, fmt.Errorf("[Copier] failed to read source symlink %s, error: %w", c.srcDir, err)
		}
		return c.preStart()
	}

	// If the top layer is a file, skip Copier and hand it over to cp util.
	// log.Printf("[Copier] DEBUG - source directory %s is file, handover to cp util\n", c.srcDir)
	return true, nil
}

func (c *Copier) dispatcher() {
	workerList := make([]string, 0, 1000)
	var dir string

	for {
		if len(workerList) == 0 {
			dir = <-c.dch
			workerList = append(workerList, dir)
		} else {
			select {
			case dir = <-c.dch:
				workerList = append(workerList, dir)
			case c.wch <- workerList[len(workerList)-1]:
				workerList = workerList[:len(workerList)-1]
			}
		}
	}
}

func (c *Copier) copy(id uint) {
	for {
		// read next directory to handle
		dir := <-c.wch

		// read current src and dest directory content
		src := path.Join(c.srcDir, dir)
		dest := path.Join(c.destDir, dir)
		// log.Printf("[Copier] DEBUG - worker %v start to handler src %s copy to dest %s\n", id, src, dest)
		files, err := os.ReadDir(src)
		if err != nil {
			log.Printf("[Copier] WARNING - failed to read directory %s, error: %v\n", src, err)
			c.wg.Done()
			continue
		}

		for _, f := range files {
			fname := f.Name()
			if fname == "." || fname == ".." {
				continue
			}

			fsrc := path.Join(src, fname)
			fdest := path.Join(dest, fname)
			if f.Type()&os.ModeSymlink == 0 && f.IsDir() {
				// if src is a single-layer directory, copy it directly
				if c.checkSingleLayer(fsrc) {
					err = cp.Copy(fsrc, fdest, c.cpOptions)
					if err != nil {
						log.Printf("[Copier] ERROR - failed to copy single-layer directory %s to %s, error: %v\n", fsrc, fdest, err)
					}
					// log.Printf("[Copier] DEBUG - copy single-layer directory %s to %s successfully\n", fsrc, fdest)
					continue
				}
				if skip, err := c.onDirExists(fsrc, fdest); err != nil {
					log.Printf("[Copier] ERROR - failed to handle cp.Options.OnDirExists in directory %s, error: %v\n", fdest, err)
					continue
				} else if skip {
					continue
				}
				finfo, err := f.Info()
				if err != nil {
					log.Printf("[Copier] ERROR - failed to get FileInfo for directory %s, error: %v\n", fdest, err)
					continue
				}
				chmodFunc, err := c.cpOptions.PermissionControl(finfo, fdest)
				if err != nil {
					log.Printf("[Copier] ERROR - failed to handle c.cpOptions.PermissionControl in directory %s, error: %v\n", fdest, err)
					continue
				}
				chmodFunc(&err)

				// submit directory to work queue
				c.wg.Add(1)
				c.dch <- path.Join(dir, fname)
				// log.Printf("[Copier] DEBUG - worker %v submit directory %s\n", id, path.Join(dir, fname))
			} else {
				// hand over the file/symlink processing to cp util
				if err = cp.Copy(fsrc, fdest, c.cpOptions); err != nil {
					log.Printf("[Copier] ERROR - failed to copy file %s to destination %s, error: %v\n", fsrc, fdest, err)
					continue
				}
				// log.Printf("[Copier] DEBUG - copy src %s to dest %s successfully. file type is %s\n", fsrc, fdest, f.Type().String())
			}
		}

		c.wg.Done()
	}
}

func (c *Copier) onDirExists(src, dest string) (bool, error) {
	_, err := os.Stat(dest)
	if err == nil && c.cpOptions.OnDirExists != nil {
		switch c.cpOptions.OnDirExists(src, dest) {
		case cp.Replace:
			if err = os.RemoveAll(dest); err != nil {
				return false, fmt.Errorf("[Copier] failed to remove directory %s, error: %w", dest, err)
			}
		case cp.Untouchable:
			return true, nil
		}
	} else if err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("[Copier] failed to stat directory %s,error: %w", dest, err)
	}
	return false, nil
}

func (c *Copier) checkSingleLayer(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("[Copier] ERROR - faled to read directory %s, error: %v\n", dir, err)
		return false
	}

	for _, entry := range entries {
		if entry.IsDir() {
			return false
		}
	}
	return true
}

func (c *Copier) readSrcSymlink() error {
	link, err := os.Readlink(c.srcDir)
	if err != nil {
		return err
	}
	// log.Printf("[Copier] DEBUG - source directory %s is symlink, it refer to %s\n", c.srcDir, link)
	if path.IsAbs(link) {
		c.srcDir = link
	} else {
		c.srcDir = path.Join(path.Join(path.Dir(c.srcDir), link))
	}
	return nil
}

func IsPathExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func MkdirWithFullPermission(path string) error {
	if IsPathExist(path) {
		return nil
	}
	err := os.MkdirAll(path, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("failed creating directory %q, %w", path, err)
	}
	// due to exist of umask, os.MkdirAll with 0777 mode MAY not grant the dir with full permission as expected.
	// see this: https://wenzr.wordpress.com/2018/03/27/go-file-permissions-on-unix/
	// it is a bit tricky here to make sure the final dir mode change to 0777 so other user from other containers can r/w in the target dir
	err = os.Chmod(path, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("failed chmod for direcotry %q, %w", path, err)
	}
	return nil
}
