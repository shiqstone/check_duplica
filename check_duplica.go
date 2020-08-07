package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func init() {
	flag.Usage = usage
}

func main() {
	timeStart := time.Now()
	var help = flag.Bool("h", false, "this help")
	var dirname = flag.String("p", "", "The directory contains all the files")
	var limit = flag.Int("m", 10, "limit the max files to caclulate.")
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	if len(*dirname) == 0 {
		fmt.Println("the directory path can not be empty")
		return
	}

	if *limit <= 0 {
		fmt.Println("the max process limit must greater than 0")
		return
	}

	result, err := Md5SumFolder(*dirname, *limit)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	md5map := make(map[string]([]string), 0)
	for path, item := range result {
		md5value := item["md5"].([md5.Size]byte)

		var ps []string
		md5str := hex.EncodeToString(md5value[:])
		if eps, ok := md5map[md5str]; ok {
			ps = eps
		}
		ps = append(ps, path)
		md5map[md5str] = ps
	}

	cnt := 0
	gidx := 0
	for _, ps := range md5map {
		if len(ps) > 1 {
			fmt.Printf("%d probablely duplica files \n", gidx)
			//exist duplica file
			for idx, path := range ps {
				size := result[path]["size"].(int64)
				fmt.Printf("%d path: %s | size=%d\n", idx, path, size)
				cnt++
			}
			fmt.Println("-------")
			gidx++
		}
	}

	fmt.Printf("overall %d group %d files probablely duplica \n", gidx, cnt)

	fmt.Println(time.Since(timeStart).String())
}

func usage() {
	fmt.Fprintf(os.Stderr, `check duplica version: 0.10.0
Usage: ./check_duplica [-h] [-p path] [-m max_process]

Options:
`)
	flag.PrintDefaults()
}

func Md5SumFile(file string) (value [md5.Size]byte, err error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}
	value = md5.Sum(data)
	return
}

type result struct {
	path   string
	md5Sum [md5.Size]byte
	size   int64
	err    error
	pdir   string
}

func Md5SumFolder(folder string, limit int) (map[string]map[string]interface{}, error) {
	returnValue := make(map[string]map[string]interface{})
	var limitChannel chan (struct{})
	if limit != 0 {
		limitChannel = make(chan struct{}, limit)
	}

	done := make(chan struct{})
	defer close(done)

	c := make(chan result)
	errc := make(chan error, 1)
	var wg sync.WaitGroup
	go func() {
		err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.Mode().IsRegular() {
				return nil
			}

			if limit != 0 {
				//block here if channel is full
				limitChannel <- struct{}{}
			}

			wg.Add(1)
			go func() {
				data, err := ioutil.ReadFile(path)
				size := info.Size()
				dir, _ := filepath.Split(path)

				select {
				case c <- result{
					path:   path,
					md5Sum: md5.Sum(data),
					size:   size,
					err:    err,
					pdir:   dir,
				}:
				case <-done:
				}
				if limit != 0 {
					//output data, than new file could be process
					<-limitChannel
				}

				wg.Done()
			}()
			select {
			case <-done:
				return errors.New("Canceled")
			default:
				return nil
			}
		})
		errc <- err
		go func() {
			wg.Wait()
			close(c)
		}()
	}()
	var kps = make(map[string]([]string), 0)
	var dsize int64 = 0

	for r := range c {
		if r.err != nil {
			return nil, r.err
		}
		returnValue[r.path] = map[string]interface{}{
			"md5":  r.md5Sum,
			"size": r.size,
		}

		dsize += r.size

		var ps []string
		if eps, ok := kps[r.pdir]; ok {
			ps = eps
		}
		ps = append(ps, r.path)
		kps[r.pdir] = ps
	}

	if err := <-errc; err != nil {
		return nil, err
	}

	return returnValue, nil
}
