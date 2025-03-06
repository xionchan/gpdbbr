package cmd

import (
	"sync"

	"golang.org/x/crypto/ssh"
)

// 命令行参数
type Config struct {
	Type       string
	DbName     string
	Jobs       int
	S3Endpoint string
	S3Id       string
	S3Key      string
	S3Bucket   string
	S3Folder   string
}

var ArgConfig Config

// SSHClient 表示一个 SSH 客户端
type SSHClient struct {
	Client *ssh.Client
}

// SSHClientPool 用于管理多个 SSH 客户端会话
type SSHClientPool struct {
	clients map[string]*SSHClient
	mu      sync.Mutex
}

var SessPool *SSHClientPool

// 源数据文件格式
// 定义顶层结构体
type BkMetaData struct {
	JobInfo             JobInfo             `yaml:"jobinfo"`
	DataEntries         []DataEntry         `yaml:"dataentries"`
	DdlSqls             []string            `yaml:"ddls"`
	IncrementalMetadata IncrementalMetadata `yaml:"incrementalmetadata"`
	FailTables          []string            `yaml:"failtables"`
	UserList            []string            `yaml:"userlist"`
	TableRows           map[string]float64  `yaml:"tablerows"`
}

// 定义 JobInfo 结构体
type JobInfo struct {
	Status    string `yaml:"status"`
	DBName    string `yaml:"dbname"`
	BeginTime string `yaml:"begintime"`
	EndTime   string `yaml:"endtime"`
}

// 定义 DataEntry 结构体
type DataEntry struct {
	TableName       string `yaml:"name"`
	OID             string `yaml:"oid"`
	AttributeString string `yaml:"attributestring"`
}

// 定义 IncrementalMetadata 结构体
type IncrementalMetadata struct {
	AO   map[string]AoMetadata   `yaml:"ao"`
	Heap map[string]HeapMetadata `yaml:"heap"`
}

// 定义 AO 表的元数据结构体
type AoMetadata struct {
	ModCount    int    `yaml:"modcount"`
	LastDDLTime string `yaml:"lastddltime"`
}

// 定义 Heap 表的统计信息结构体
type HeapMetadata struct {
	MaxStat int `yaml:"maxstat"`
}

type RsRpt struct {
	Status     string   `yaml:"status"`
	BeginTime  string   `yaml:"begintime"`
	EndTime    string   `yaml:"endtime"`
	FailTables []string `yaml:"failtables"`
	FailDDLs   []string `yaml:"failddl"`
}
