package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var file *os.File
var skip int64
var ignoreErr bool
var tvType string
var tvFile *os.File

const filechunk = 8192
const bigFileSize = 512 * 1024 * 1024

func init() {
	flag.Usage = usage
}

func main() {
	timeStart := time.Now()
	var help = flag.Bool("h", false, "this help")
	var dirname = flag.String("p", "", "the directory contains all the files")
	var limit = flag.Int("m", 10, "limit the max files to caclulate.")
	var output = flag.String("o", "", "the path of output check result, not save when set empty")
	var debug = flag.Bool("debug", false, "is run debug mode")
	flag.Int64Var(&skip, "skip", 0, "skip file size small than set sizes, unit KB")
	flag.BoolVar(&ignoreErr, "i", true, "ignore filepath permit errors and skip")
	flag.StringVar(&tvType, "tvt", "mem", "temp value type, file/mem default mem")
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}
	if *debug {
		go func() {
			log.Println(http.ListenAndServe("0.0.0.0:10000", nil))
		}()
	}

	if len(*dirname) == 0 {
		fmt.Println("the directory path can not be empty")
		return
	}

	if *limit <= 0 {
		fmt.Println("the max process limit must greater than 0")
		return
	}

	if len(*output) > 0 {
		var err error
		file, err = os.Create(*output)
		if err != nil {
			log.Fatalln("fail to create output file!")
		}
		defer file.Close()
	}
	if tvType == "file" {
		var err error
		tvFile, err = ioutil.TempFile("./", "returnValue-")
		if err != nil {
			log.Fatal("fail to create temp value file", err)
		}
		defer func() {
			tvFile.Close()
			err := os.Remove(tvFile.Name())
			if err != nil {
				log.Fatal(err.Error())
			}
		}()
	}

	skip *= 1024

	result, err := Md5SumFolder(*dirname, *limit)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	md5map := make(map[string]([]string), 0)
	sizemap := make(map[string]int, 0)
	if tvType == "file" {
		f, err := os.Open(tvFile.Name())
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		defer f.Close()
		br := bufio.NewReader(f)
		for {
			lines, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			items := strings.Split(string(lines), "||")
			path := items[0]
			md5str := items[1]
			var ps []string
			if eps, ok := md5map[md5str]; ok {
				ps = eps
			}
			ps = append(ps, path)
			md5map[md5str] = ps
			sizemap[md5str], _ = strconv.Atoi(items[2])
		}
	} else {
		for path, item := range result {
			md5value := item["md5"].([]byte)

			var ps []string
			md5str := hex.EncodeToString(md5value)
			if eps, ok := md5map[md5str]; ok {
				ps = eps
			}
			ps = append(ps, path)
			md5map[md5str] = ps
		}
	}

	cnt := 0
	gidx := 0
	for md5k, ps := range md5map {
		if len(ps) > 1 {
			logOutput("%d probablely duplica files \n", gidx)
			//exist duplica file
			for idx, path := range ps {
				if tvType == "file" {
					size := int64(sizemap[md5k])
					logOutput("%d path: %s | size=%d\n", idx, path, size)
				} else {
					size := result[path]["size"].(int64)
					logOutput("%d path: %s | size=%d\n", idx, path, size)
				}
				cnt++
			}
			logOutput("-------%s\n", "-")
			gidx++
		}
	}

	logOutput("overall %d group %d files probablely duplica \n", gidx, cnt)

	fmt.Println(time.Since(timeStart).String())
}

func usage() {
	fmt.Fprintf(os.Stderr, `check duplica version: 0.10.0
Usage: ./check_duplica [-h] [-p path] [-m max_process]

Options:
`)
	flag.PrintDefaults()
}

func logOutput(f string, v ...interface{}) {
	fmt.Printf(f, v...)
	wstring := fmt.Sprintf(f, v...)
	if file != nil {
		_, err := io.WriteString(file, wstring)
		if err != nil {
			log.Fatalln("fail to write output file!")
		}
	}
}

func Md5SumFile(filepath string) (value []byte, err error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return
	}
	sum := md5.Sum(data)
	value = sum[:]
	return
}

func Md5SumBigFile(filepath string, filesize int64) (value []byte, err error) {
	file, err := os.Open(filepath)
	if err != nil {
		return
	}
	defer file.Close()
	blocks := uint64(math.Ceil(float64(filesize) / float64(filechunk)))
	hash := md5.New()
	for i := uint64(0); i < blocks; i++ {
		blocksize := int(math.Min(filechunk, float64(filesize-int64(i*filechunk))))
		buf := make([]byte, blocksize)
		file.Read(buf)
		io.WriteString(hash, string(buf)) // append into the hash
	}
	return hash.Sum(nil), err
}

type result struct {
	path   string
	md5Sum []byte
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
				if ignoreErr {
					if errObject, ok := err.(*os.PathError); ok {
						//ignore file path error and continue
						fmt.Println(errObject.Path, err)
						return nil
					}
				}
				return err
			}

			if !info.Mode().IsRegular() {
				return nil
			}

			size := info.Size()
			if skip > 0 && size < skip {
				return nil
			}

			if limit != 0 {
				//block here if channel is full
				limitChannel <- struct{}{}
			}

			wg.Add(1)
			go func() {
				//data, err := ioutil.ReadFile(path)
				dir, _ := filepath.Split(path)
				var md5v []byte
				if size > bigFileSize {
					md5v, err = Md5SumBigFile(path, size)
				} else {
					md5v, err = Md5SumFile(path)
				}

				select {
				case c <- result{
					path:   path,
					md5Sum: md5v, //md5.Sum(data),
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
	//var kps = make(map[string]([]string), 0)
	var dsize int64 = 0

	for r := range c {
		if r.err != nil {
			if ignoreErr {
				if errObject, ok := r.err.(*os.PathError); ok {
					//ignore file path error and continue
					fmt.Println(errObject.Path, r.err)
					continue
				}
			}
			return nil, r.err
		}
		if tvType == "file" {
			wstring := fmt.Sprintf("%s||%s||%d\n", r.path, hex.EncodeToString(r.md5Sum[:]), r.size)
			if tvFile != nil {
				_, err := tvFile.Write([]byte(wstring))
				if err != nil {
					log.Fatalln("fail to write temp value file!")
				}
			}
		} else {
			returnValue[r.path] = map[string]interface{}{
				"md5":  r.md5Sum,
				"size": r.size,
			}
		}

		dsize += r.size

		// var ps []string
		// if eps, ok := kps[r.pdir]; ok {
		// 	ps = eps
		// }
		// ps = append(ps, r.path)
		// kps[r.pdir] = ps
	}

	if err := <-errc; err != nil {
		return nil, err
	}

	return returnValue, nil
}
