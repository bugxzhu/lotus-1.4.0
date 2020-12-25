package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"context"

	"github.com/docker/go-units"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

var log = logging.Logger("lotus-c2server")

func main() {
	remoteAddress := flag.String("remote", "127.0.0.1:16800", "scheduler http listen address")
	localAddress := flag.String("local", "127.0.0.1:26800", "c2 host http listen address")
	workPath := flag.String("workpath", "~/.c2path", "c2 work path")
	flag.Parse()

	os.Setenv("C2_PATH", *workPath)
	go Register(*remoteAddress, *localAddress)

	fmt.Println("C2 计算主机启动: ", *localAddress)
	http.HandleFunc("/commit2/ping", Pong)
	http.HandleFunc("/commit2/task", HandleCommit2)
	http.ListenAndServe(*localAddress, nil)
}

// Register C2主机向调度器注册
func Register(remoteAddress string, localAddress string) {
	for {
		req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/proxy/register", remoteAddress), strings.NewReader(localAddress))
		if err != nil {
			fmt.Print("Register: http.NewRequest ", err.Error())
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Print(err)
		}
		resp.Body.Close()
		time.Sleep(30 * time.Second)
	}
}

// Pong 回应调度器的存活请求
func Pong(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, "pong")
}

// HandleCommit2 调用C2处理函数
func HandleCommit2(w http.ResponseWriter, req *http.Request) {
	fmt.Println("test============== 接收到C2任务")
	reqBody, err := gzip.NewReader(req.Body)
	if err != nil {
		log.Errorf("gzip commit2 unzip failed, error: %+v", err)
	}
	defer reqBody.Close()
	undatas, err := ioutil.ReadAll(reqBody) // 解压
	if err != nil {
		log.Errorf("ioutil read c2 request body err: %+v", err)
		return
	}
	fmt.Println("test============== 解压C2任务成功")

	request := Commit2Request{}
	if err := json.Unmarshal(undatas, &request); err != nil { // 解析c1输出结果到request结构体变量中
		log.Errorf("request json unmarshel error: %+v", err)
		return
	}
	fmt.Println("test============== 解析扇区 ", request.SectorID.Number, " C2任务成功，开始做证明......")

	proof, err := doC2Job(request) // 调用c2函数做任务
	if err != nil {
	}
	fmt.Println("test============== C2任务完成")

	response := Commit2Response{ // 返回结构体
		SectorID: request.SectorID,
		Proof:    proof,
	}

	jsonResponse, err := json.Marshal(response) // 将返回结果json序列化
	if err != nil {
		log.Errorf("response json marshel error: %+v", err)
		return
	}
	io.WriteString(w, string(jsonResponse)) // io.Copy(w, string(jsonResponse))
	fmt.Println("test============== 返回C2任务......")
}

// Commit2Request C2请求结构体
type Commit2Request struct {
	SectorID   abi.SectorID
	Commit1Out storage.Commit1Out
}

// Commit2Response C2结果响应结构体
type Commit2Response struct {
	SectorID abi.SectorID
	Proof    storage.Proof
}

// doC2Job 做C2任务
func doC2Job(c2Req Commit2Request) (storage.Proof, error) {
	rootPath, _ := os.LookupEnv("C2_PATH")
	sbfs := &basicfs.Provider{
		Root: rootPath,
	}
	sb, err := ffiwrapper.New(sbfs)
	if err != nil {
		return nil, err
	}

	sectorSizeInt, err := units.RAMInBytes("32GiB")
	if err != nil {
		return nil, err
	}
	sectorSize := abi.SectorSize(sectorSizeInt)
	spt, err := miner.SealProofTypeFromSectorSize(sectorSize, build.NewestNetworkVersion)
	// spt, err := miner.SealProofTypeFromSectorSize(abi.SectorSize(sectorSize), genesis.GenesisNetworkVersion)
	if err != nil {
		panic(err)
	}
	sid := storage.SectorRef{
		ID:        c2Req.SectorID,
		ProofType: spt,
	}

	return sb.SealCommit2(context.TODO(), sid, c2Req.Commit1Out)
}
