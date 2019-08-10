package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

func main() {
	fmt.Println("Digester...")

	fmt.Println("Serial digester...")
	// calculate the MD5 sum of all files under the specified directory,
	// then print the results sorted by path name
	m, err := MD5AllSerial(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	var paths []string
	for path := range m {
		paths = append(paths, path)
	}

	sort.Strings(paths)
	for _, path := range paths {
		fmt.Println(fmt.Sprintf("%x %s", m[path], path))
	}

	fmt.Println("Parallel digester...")
	// calculate the MD5 sum of all files under the specified directory,
	// then print the results sorted by path name
	m, err = MD5AllParallel(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	paths = []string{}
	for path := range m {
		paths = append(paths, path)
	}

	sort.Strings(paths)
	for _, path := range paths {
		fmt.Println(fmt.Sprintf("%x %s", m[path], path))
	}
}

// MD5AllSerial reads all the files in the file tree rooted at root and returns a map
// from file path to the MD5 sum of the file's contents.  If the directory walk
// fails or any read operation fails, MD5All returns an error.
func MD5AllSerial(root string) (map[string][md5.Size]byte, error) {
	m := make(map[string][md5.Size]byte)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		m[path] = md5.Sum(data)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return m, nil
}

// MD5AllParallel closes the done channel when it returns; it may do so before
// receiving all the values from c and errc.
func MD5AllParallel(root string) (map[string][md5.Size]byte, error) {
	done := make(chan struct{})
	defer close(done)

	c, errc := sumFiles(done, root)

	m := make(map[string][md5.Size]byte)
	for r := range c {
		if r.err != nil {
			return nil, r.err
		}
		m[r.path] = r.sum
	}
	if err := <-errc; err != nil {
		return nil, err
	}
	return m, nil
}

type result struct {
	path string
	sum  [md5.Size]byte
	err  error
}

func sumFiles(done <-chan struct{}, root string) (<-chan result, <-chan error) {
	// for each regular file, start a goroutine that sums the file and sends the result on c
	// send the result of the walk on errc
	c := make(chan result)
	errc := make(chan error, 1)

	go func() {
		var wg sync.WaitGroup
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.Mode().IsRegular() {
				return nil
			}

			wg.Add(1)
			go func() {
				data, err := ioutil.ReadFile(path)
				select {
				case c <- result{path, md5.Sum(data), err}:
				case <-done:
				}
				wg.Done()
			}()

			// abort the walk if done is closed
			select {
			case <-done:
				return errors.New("walk cancelled")
			default:
				return nil
			}
		})

		// Walk has returned, so all calls to wg.Add are done
		// start a goroutine to close c once all the sends are done
		go func() {
			wg.Wait()
			close(c)
		}()

		// no select needed here, since errc is buffered
		errc <- err
	}()

	return c, errc
}
