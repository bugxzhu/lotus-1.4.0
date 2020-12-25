package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("lotus-c2scheduler")

// C2Proxy 代理主机清单结构体
type C2Proxy struct {
	HostListLk sync.RWMutex
	HostList   map[string]*HostInfo
}

// HostInfo C2主机信息结构体
type HostInfo struct {
	Active   bool
	StatTime time.Time
}

// GlobleProxy 全局变量
var GlobleProxy C2Proxy

// ProxyInit 初始化全局变量
func (p *C2Proxy) ProxyInit() {
	p.HostListLk.Lock()
	p.HostList = make(map[string]*HostInfo)
	p.HostListLk.Unlock()
}

// AddHost 添加c2主机，存在则忽略
func (p *C2Proxy) AddHost(key string) {
	p.HostListLk.Lock()
	defer p.HostListLk.Unlock()
	if _, exist := p.HostList[key]; !exist {
		p.HostList[key] = &HostInfo{
			Active:   true,
			StatTime: time.Now(),
		}
		fmt.Println("test==============", key, "接入注册成功")
	}
}

// DeleteHost 删除C2主机
func (p *C2Proxy) DeleteHost(key string) {
	p.HostListLk.Lock()
	defer p.HostListLk.Unlock()
	delete(p.HostList, key)
}

// DisableHost 去使能C2主机
func (p *C2Proxy) DisableHost(key string) {
	p.HostListLk.Lock()
	defer p.HostListLk.Unlock()
	p.HostList[key] = &HostInfo{
		Active:   false,
		StatTime: time.Now(),
	}
}

// EnableHost 使能C2主机
func (p *C2Proxy) EnableHost(key string) {
	p.HostListLk.Lock()
	defer p.HostListLk.Unlock()
	p.HostList[key] = &HostInfo{
		Active:   true,
		StatTime: time.Now(),
	}
}

// GetFreeHost0 获取空闲的C2主机
func (p *C2Proxy) GetFreeHost0() string {
	p.HostListLk.Lock()
	defer p.HostListLk.Unlock()
	for host, hostInfo := range GlobleProxy.HostList {
		if hostInfo.Active {
			return host
		}
	}
	return ""
}

// GetFreeHost 获取空闲的C2主机
func (p *C2Proxy) GetFreeHost() string {
	p.HostListLk.Lock()
	defer p.HostListLk.Unlock()
	hostList := make([]string, 0)
	for k := range p.HostList { //先将主机清单copy出来
		hostList = append(hostList, k)
	}

	for _, host := range hostList { // 检测c2主机是否存活
		if !checkHostHeart(host) {
			delete(p.HostList, host)
			continue
		}
		hostInfo := GlobleProxy.HostList[host]
		if hostInfo.Active {
			return host
		}
	}

	return ""
}

func checkHostHeart(host string) bool {
	resp, err := http.Get(fmt.Sprintf("http://%s/commit2/ping", host))
	if err != nil {
		fmt.Println("test==============", host, "1心跳检测失败")
		fmt.Println(err)
		return false
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("test==============", host, "2心跳检测失败")
		fmt.Println(err)
		return false
	}
	if string(respBody) != "pong" {
		fmt.Println("test==============", host, "3心跳检测失败")
		fmt.Println("ping err: ", host)
		return false
	}
	resp.Body.Close()
	fmt.Println("test==============", host, "心跳检测正常")
	return true
}

func main() {
	localAddress := flag.String("local", "127.0.0.1:16800", "http listen address")
	flag.Parse()

	GlobleProxy.ProxyInit()
	// go checkHeart()

	fmt.Println("调度器启动: ", *localAddress)
	http.HandleFunc("/proxy/register", HandleRegister)
	http.HandleFunc("/proxy/commit2", ProxyCommit2)
	http.ListenAndServe(*localAddress, nil)
}

func checkHeart() {
	for {
		hostList := make([]string, 0)

		GlobleProxy.HostListLk.Lock()
		for k := range GlobleProxy.HostList { // 因为锁，先将主机清单copy出来
			hostList = append(hostList, k)
		}
		GlobleProxy.HostListLk.Unlock()

		for _, host := range hostList { // 检测c2主机是否存活
			resp, err := http.Get(fmt.Sprintf("http://%s/commit2/ping", host))
			if err != nil {
				GlobleProxy.DeleteHost(host)
				fmt.Println("test==============", host, "1心跳检测失败")
				fmt.Println(err)
				continue
			}
			respBody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				GlobleProxy.DeleteHost(host)
				fmt.Println("test==============", host, "2心跳检测失败")
				fmt.Println(err)
				continue
			}
			if string(respBody) != "pong" {
				GlobleProxy.DeleteHost(host)
				fmt.Println("test==============", host, "3心跳检测失败")
				fmt.Println("ping err: ", host)
			}
			resp.Body.Close()
			fmt.Println("test==============", host, "心跳检测正常")
		}

		time.Sleep(15 * time.Second)
	}
}

// HandleRegister c2主机向代理调度器接入注册(间隔10s一次)
func HandleRegister(w http.ResponseWriter, req *http.Request) {
	reqBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errorf("HandleRegister request body err: %+v", err)
		return
	}
	GlobleProxy.AddHost(string(reqBody)) // 添加主机记录，存在则不理会
	io.WriteString(w, "Register ok!")
}

// ProxyCommit2 转发miner发过来的c2到c2计算机器上
func ProxyCommit2(w http.ResponseWriter, r *http.Request) {
	// body, err := ioutil.ReadAll(r.Body)
	// if err != nil {
	// 	fmt.Println("Proxy: ioutil.ReadAll(r.Body) ", err.Error())
	// }

	var host, hostURL string
	for { // 一直找到可用c2主机为止
		host = GlobleProxy.GetFreeHost()
		if host != "" {
			GlobleProxy.DisableHost(host) // 找到可用c2主机后，去使能
			hostURL = fmt.Sprintf("http://%s/commit2/task", host)
			break
		}
		time.Sleep(6 * time.Second)
	}

	// req, err := http.NewRequest(r.Method, hostURL, strings.NewReader(string(body)))
	request, err := http.NewRequest(r.Method, hostURL, r.Body)
	if err != nil {
		fmt.Print("Proxy: http.NewRequest ", err.Error())
		return
	}

	for k, v := range r.Header {
		request.Header.Set(k, v[0])
	}
	fmt.Println("test============== 转发", r.URL.Host, "C2任务到", host)
	resp, err := http.DefaultClient.Do(request) // 转发到c2主机(要等待，可能要很久)
	GlobleProxy.EnableHost(host)                // 做完c2任务后使能
	if err != nil {
		fmt.Print("Proxy: http.DefaultClient.Do(request) ", err.Error())
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		w.Header().Set(k, v[0])
	}
	io.Copy(w, resp.Body) // 将c2主机的结果返回给miner
}
