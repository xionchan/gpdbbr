package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"
)

// 自定义日志函数
func LogInfo(format string, v ...interface{}) {
	logWithTag("INFO", format, v...)
}

func LogError(format string, v ...interface{}) {
	logWithTag("ERROR", format, v...)
}

// 通用的日志打印函数
func logWithTag(tag string, format string, v ...interface{}) {
	// 获取当前时间，格式化为 yyyymmdd hh24:mi:ss
	timestamp := time.Now().Format("20060102 15:04:05")

	// 拼接日志内容
	message := fmt.Sprintf(format, v...)

	// 如果是 ERROR 日志，获取调用栈信息
	if tag == "ERROR" {
		// 获取调用栈信息
		_, file, line, ok := runtime.Caller(2) // 2 表示跳过两层调用栈（logWithTag 和 LogError）
		if ok {
			message = fmt.Sprintf("%s:%d - %s", file, line, message)
		}
	}

	// 打印日志
	fmt.Printf("%s gpdbbr [%s]:- %s\n", timestamp, tag, message)
}

// 创建本地数据库连接
func CreateDbConn(dbname string) *sql.DB {
	var db *sql.DB

	connStr := fmt.Sprintf("host=localhost port=5432 user=gpadmin dbname=%s sslmode=disable", dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		LogError("Failed to connect database(%s): %s", dbname, err.Error())
		os.Exit(1)
	}
	return db
}

// 执行操作系统命令
func ExecOsCmd(oscmd string, oscmdargs []string) (bool, string) {
	oscmdall := exec.Command(oscmd, oscmdargs...)

	var stdout, stderr bytes.Buffer
	oscmdall.Stdout = &stdout
	oscmdall.Stderr = &stderr

	if err := oscmdall.Run(); err != nil {
		return false, stderr.String()
	}

	return true, stdout.String()
}

// 上传文件到s3
func PutFileToS3(filePath string, objectKey string) {
	s3client := CreS3Client()

	file, err := os.Open(filePath)
	if err != nil {
		LogError("Failed to open file(%s) for put file to s3: %s", filePath, err.Error())
		os.Exit(1)
	}
	defer file.Close()

	// 获取文件大小
	fileInfo, err := file.Stat()
	if err != nil {
		LogError("Failed to get file(%s) status for put file to s3: %s", filePath, err.Error())
		os.Exit(1)
	}

	ctx := context.Background()
	_, err = s3client.PutObject(ctx, ArgConfig.S3Bucket, objectKey, file, fileInfo.Size(), minio.PutObjectOptions{ContentType: "application/octest-stream"})
	if err != nil {
		LogError("Failed to put file(%s) to s3: %s", filePath, err.Error())
		os.Exit(1)
	}
}

func InitSess(hostlist []string) {
	SessPool = NewSSHClientPool()

	for _, host := range hostlist {
		_, err := SessPool.GetClient(host)
		if err != nil {
			LogError("Failed to create ssh client for host(%s): %s", host, err.Error())
			os.Exit(1)
		}
	}
}

// NewSSHClientPool 创建一个新的 SSHClientPool
func NewSSHClientPool() *SSHClientPool {
	return &SSHClientPool{
		clients: make(map[string]*SSHClient),
	}
}

// GetClient 获取指定主机的 SSH 客户端
func (p *SSHClientPool) GetClient(host string) (*SSHClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if client, ok := p.clients[host]; ok {
		return client, nil
	}

	// 创建新的 SSH 客户端
	client, err := createSSHClient(host)
	if err != nil {
		return nil, err
	}

	p.clients[host] = &SSHClient{Client: client}
	return p.clients[host], nil
}

// createSSHClient 创建一个新的 SSH 客户端
func createSSHClient(host string) (*ssh.Client, error) {
	// 使用当前用户的私钥进行认证
	key, err := os.ReadFile(os.Getenv("HOME") + "/.ssh/id_rsa")
	if err != nil {
		return nil, fmt.Errorf("unable to read private key: %v", err)
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("unable to parse private key: %v", err)
	}

	config := &ssh.ClientConfig{
		User: "gpadmin", // 替换为你的用户名
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 忽略主机密钥检查
	}

	client, err := ssh.Dial("tcp", host+":22", config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to %s: %v", host, err)
	}

	return client, nil
}

// ExecuteCommand 在指定主机上执行命令
func (p *SSHClientPool) ExecuteCommand(host, command string) (string, error) {
	client, err := p.GetClient(host)
	if err != nil {
		return "", err
	}

	session, err := client.Client.NewSession()
	if err != nil {
		return "", fmt.Errorf("unable to create session: %v", err)
	}
	defer session.Close()

	output, err := session.CombinedOutput(command)
	if err != nil {
		return "", fmt.Errorf("command execution failed: %v", err)
	}

	return strings.TrimRight(string(output), "\n"), nil
}

// 处理SQL结果集
// ProcessRows函数用于处理sql.Rows类型的行数据，返回一个map[string]interface{}类型的切片
func ProcessRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 定义一个map[string]interface{}类型的切片
	var valuemap []map[string]interface{}

	// 遍历每一行数据
	for rows.Next() {
		// 定义一个interface{}类型的切片，用于存储每一行的数据
		values := make([]interface{}, len(columns))
		// 定义一个interface{}类型的切片，用于存储每一行的数据指针
		valuePtrs := make([]interface{}, len(columns))

		// 将每一行的数据指针存储到valuePtrs切片中
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 将每一行的数据存储到values切片中
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// 定义一个map[string]interface{}类型的map，用于存储每一行的数据
		rowMap := make(map[string]interface{})
		// 遍历每一列的数据
		for i, colName := range columns {
			// 获取每一列的数据
			val := values[i]

			// 如果数据类型是[]byte，则将其转换为string类型
			if b, ok := val.([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				// 否则，直接将数据存储到map中
				rowMap[colName] = val
			}
		}

		// 将每一行的数据存储到valuemap切片中
		valuemap = append(valuemap, rowMap)
	}

	// 返回valuemap切片
	return valuemap, nil
}

// 切片合并函数
func MergeSlices(slice1, slice2 []map[string]interface{}, commarg string) []map[string]interface{} {
	// 创建一个map用来快速查找slice2的元素
	map2 := make(map[interface{}]map[string]interface{})
	for _, item := range slice2 {
		if key, exists := item[commarg]; exists {
			map2[key] = item
		}
	}

	// 遍历slice1并追加匹配到的元素
	for i, item := range slice1 {
		if key, exists := item[commarg]; exists {
			if matched, found := map2[key]; found {
				// 将 slice2 中的键值对追加到 slice1 中，排除 commarg
				for k, v := range matched {
					if k != commarg {
						slice1[i][k] = v
					}
				}
			}
		}
	}

	return slice1
}

// 下发s3配置文件
func CreS3Yaml(ts string, gphome string, hostlist []string) {
	s3config := map[string]interface{}{
		"executablepath": fmt.Sprintf("%s/bin/gpbackup_s3_plugin", gphome),
		"options": map[string]interface{}{
			"aws_access_key_id":     ArgConfig.S3Id,
			"aws_secret_access_key": ArgConfig.S3Key,
			"bucket":                ArgConfig.S3Bucket,
			"endpoint":              fmt.Sprintf("http://%s", ArgConfig.S3Endpoint),
			"folder":                ArgConfig.S3Folder,
		},
	}

	yamls3config, err := yaml.Marshal(s3config)
	if err != nil {
		LogError("Failed to marshal s3 config: %s", err.Error())
		os.Exit(1)
	}

	file, err := os.Create(fmt.Sprintf("/tmp/gpdbbr_%s_s3.yaml", ts))
	if err != nil {
		LogError("Failed to create s3 config file(%s): %s", fmt.Sprintf("/tmp/gpdbbr_%s_s3.yaml", ts), err.Error())
		os.Exit(1)
	}
	defer file.Close()

	_, err = file.Write(yamls3config)
	if err != nil {
		LogError("Failed to write s3 config file(%s): %s", fmt.Sprintf("/tmp/gpdbbr_%s_s3.yaml", ts), err.Error())
		os.Exit(1)
	}

	// 下发文件
	for _, host := range hostlist {
		ok, output := ExecOsCmd("scp", []string{fmt.Sprintf("/tmp/gpdbbr_%s_s3.yaml", ts), fmt.Sprintf("%s:/tmp/gpdbbr_%s_s3.yaml", host, ts)})
		if !ok {
			LogError("Failed to issue s3 config file to host(%s): %s", host, output)
			os.Exit(1)
		}
	}
}

func CreS3Client() *minio.Client {
	s3client, err := minio.New(ArgConfig.S3Endpoint, &minio.Options{Creds: credentials.NewStaticV4(
		ArgConfig.S3Id, ArgConfig.S3Key, ""), Secure: false})
	if err != nil {
		LogError("Failed to create s3 client: %s", err.Error())
		os.Exit(1)
	}

	return s3client
}
