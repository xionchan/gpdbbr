package cmd

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
)

func customUsage() {
	fmt.Fprintf(os.Stderr, "Usage: gpdbbr [OPTIONS]\n\n")
	fmt.Fprintf(os.Stderr, "Options:\n")
	fmt.Fprintf(os.Stderr, "  --type string          Command type, backup, restore or check (required)\n")
	fmt.Fprintf(os.Stderr, "  --dbname string        Database name (required)\n")
	fmt.Fprintf(os.Stderr, "  --jobs int             Parallel jobs [1-64] (optional, default: 1)\n")
	fmt.Fprintf(os.Stderr, "  --s3endpoint string    S3 endpoint (required)\n")
	fmt.Fprintf(os.Stderr, "  --s3id string          S3 access key ID (required)\n")
	fmt.Fprintf(os.Stderr, "  --s3key string         S3 access key (required)\n")
	fmt.Fprintf(os.Stderr, "  --s3bucket string      S3 bucket name (required)\n")
	fmt.Fprintf(os.Stderr, "  --s3folder string      S3 folder name (required)\n\n")
	fmt.Fprintf(os.Stderr, "Example:\n")
	fmt.Fprintf(os.Stderr, "  gpdbbr --type backup --dbname chenxw --jobs 2 --s3endpoint '10.187.112.1:1521' --s3id admin --s3key password --s3bucket test --s3folder chenxw\n")
}

func ParseArg() {
	cmdType := flag.String("type", "", "command type, backup, restore or check (required)")
	dbname := flag.String("dbname", "", "database name (required)")
	jobs := flag.Int("jobs", 1, "parallel jobs [1-64] (optional, default: 1)")
	s3endpoint := flag.String("s3endpoint", "", "s3 endpoint (required)")
	s3id := flag.String("s3id", "", "s3 access key id (required)")
	s3key := flag.String("s3key", "", "s3 access key (required)")
	s3bucket := flag.String("s3bucket", "", "s3 bucket name (required)")
	s3folder := flag.String("s3folder", "", "s3 folder name (required)")

	flag.Usage = customUsage
	flag.Parse()

	if *cmdType == "" {
		log.Printf("Error: Missing required argument: --type\n")
		flag.Usage()
		os.Exit(1)
	} else if *cmdType != "backup" && *cmdType != "restore" && *cmdType != "check" {
		log.Printf("Error: Invalid argument: --type, must be backup, restore or check\n")
		flag.Usage()
		os.Exit(1)
	}

	if *dbname == "" {
		log.Printf("Error: Missing required argument: --dbname\n")
		flag.Usage()
		os.Exit(1)
	}

	if *s3endpoint == "" {
		log.Printf("Error: Missing required argument: --s3endpoint\n")
		flag.Usage()
		os.Exit(1)
	}

	if *s3id == "" {
		log.Printf("Error: Missing required argument: --s3id\n")
		flag.Usage()
		os.Exit(1)
	}

	if *s3key == "" {
		log.Printf("Error: Missing required argument: --s3key\n")
		flag.Usage()
		os.Exit(1)
	}

	if *s3bucket == "" {
		log.Printf("Error: Missing required argument: --s3bucket\n")
		flag.Usage()
		os.Exit(1)
	}

	if *s3folder == "" {
		log.Printf("Error: Missing required argument: --s3folder\n")
		flag.Usage()
		os.Exit(1)
	}

	if *jobs < 1 || *jobs > 64 {
		log.Printf("Error: jobs must be in range [1-64]\n")
		flag.Usage()
		os.Exit(1)
	}

	ArgConfig = Config{
		Type:       *cmdType,
		DbName:     *dbname,
		Jobs:       *jobs,
		S3Endpoint: *s3endpoint,
		S3Id:       *s3id,
		S3Key:      *s3key,
		S3Bucket:   *s3bucket,
		S3Folder:   *s3folder,
	}

	// 打印软件版本
	LogInfo("gpdbbr version = 0.0.2")

	// 打印任务类型
	LogInfo("job type = %s", ArgConfig.Type)

	// 测试s3的连通性, bucket是否存在
	s3client := CreS3Client()

	exists, err := s3client.BucketExists(context.Background(), ArgConfig.S3Bucket)
	if err != nil {
		LogError("Failed to check s3 bucket exist: %s", err.Error())
		os.Exit(1)
	}

	if !exists {
		LogError("The s3 bucket(%s) dest not exist", ArgConfig.S3Bucket)
		os.Exit(1)
	}
}
