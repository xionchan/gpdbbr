package restore

import (
	"context"
	"fmt"
	"gpdbbr/cmd"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"gopkg.in/yaml.v3"
)

func DoRestore() {
	isbk := getrstype()
	if !isbk {
		cmd.LogInfo("Restore completed successfully")
		return
	}

	commrestore()

	// 写入报告信息
	if len(RestoreRpt.FailTables) == 0 && len(RestoreRpt.FailDDLs) == 0 {
		RestoreRpt.Status = "success"
	} else {
		RestoreRpt.Status = "failed"
	}
	RestoreRpt.EndTime = time.Now().Format("20060102150405000")
	err := os.MkdirAll(fmt.Sprintf("%s/gpdbbr/%s/%s/%s/", CnDir, cmd.ArgConfig.DbName, RestoreDate, RestoreTime), 0755)
	if err != nil {
		cmd.LogError("Failed to create restore report dir: %s", err.Error())
		os.Exit(1)
	}
	osfile := fmt.Sprintf("%s/gpdbbr/%s/%s/%s/gpdbbr_%s_report", CnDir, cmd.ArgConfig.DbName, RestoreDate, RestoreTime, RestoreTime)
	yamldata, err := yaml.Marshal(&RestoreRpt)
	if err != nil {
		cmd.LogError("Failed to marshal restore report: %s", err.Error())
		os.Exit(1)
	}
	err = os.WriteFile(osfile, yamldata, 0644)
	if err != nil {
		cmd.LogError("Failed to write restore report: %s", err.Error())
		os.Exit(1)
	}
	cmd.LogInfo("Restore report: %s", osfile)

	if len(RestoreRpt.FailTables) > 0 || len(RestoreRpt.FailDDLs) > 0 {
		cmd.LogInfo("Restore completed with some errors")
	} else {
		cmd.LogInfo("Restore completed successfully")
	}

}

func getrstype() bool {
	// 校验备份类型
	cmd.LogInfo("Checking restore type")
	// 获取COORDINATOR_DATA_DIRECTORY环境变量, 判断是否存在dbname文件
	CnDir = os.Getenv("COORDINATOR_DATA_DIRECTORY")
	if CnDir == "" {
		cmd.LogError("COORDINATOR_DATA_DIRECTORY environment variable not set")
		os.Exit(1)
	}

	predate := 0
	pretime := 0
	rsdir := fmt.Sprintf("%s/gpdbbr/%s", CnDir, cmd.ArgConfig.DbName)
	_, err := os.Stat(rsdir)
	if err != nil {
		if os.IsNotExist(err) {
			RestoreType = "full"
			cmd.LogInfo("Restore type = full restore")
		} else {
			cmd.LogError("Failed to get restore type: %s", err.Error())
			os.Exit(1)
		}
	} else {
		// 校验还原目录
		pattern := filepath.Join(rsdir, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")
		matches, err := filepath.Glob(pattern)
		if err != nil {
			cmd.LogError("Failed to check restore directory: %s", err.Error())
			os.Exit(1)
		}

		if len(matches) == 0 {
			RestoreType = "full"
			cmd.LogInfo("Restore type = full restore")
		} else {
			RestoreType = "increment"
			cmd.LogInfo("Restore type = incremental restore")
			// 按文件名降序排序
			sort.Sort(sort.Reverse(sort.StringSlice(matches)))
			predate, _ = strconv.Atoi(filepath.Base(matches[0]))

			// 获取timestamp部分
			datedir := fmt.Sprintf("%s/%d", rsdir, predate)
			pattern = filepath.Join(datedir, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")
			matches, err = filepath.Glob(pattern)
			if err != nil {
				cmd.LogError("Failed to check restore directory: %s", err.Error())
				os.Exit(1)
			}

			if len(matches) == 0 {
				cmd.LogError("Failed to check restore directory: no timestamp dir found")
				os.Exit(1)
			} else {
				// 按文件名降序排序
				sort.Sort(sort.Reverse(sort.StringSlice(matches)))
				pretime, _ = strconv.Atoi(filepath.Base(matches[0]))
				// 判断是否有rowchk文件
				rowchkfile := fmt.Sprintf("%s/%d/rowchk_%d_report", datedir, pretime, pretime)
				_, err := os.Stat(rowchkfile)
				if err == nil {
					cmd.LogError("Rowchk file exists, please check the log file")
					os.Exit(1)
				}

				prerptfile := fmt.Sprintf("%s/%d/gpdbbr_%d_report", datedir, pretime, pretime)

				// 解析文件
				var prerpt cmd.RsRpt
				data, err := os.ReadFile(prerptfile)
				if err != nil {
					cmd.LogError("Failed to read restore report: %s", err.Error())
					os.Exit(1)
				}

				err = yaml.Unmarshal(data, &prerpt)
				if err != nil {
					cmd.LogError("Failed to parse restore report: %s", err.Error())
					os.Exit(1)
				}

				if prerpt.Status != "success" {
					cmd.LogError("Previous restore failed, please check the log file")
					os.Exit(1)
				}
			}
		}
	}

	// s3校验
	s3client := cmd.CreS3Client()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if RestoreType == "increment" {
		// 列出日期下的时间戳
		var timedirs []int
		timedirobjects := s3client.ListObjects(ctx, cmd.ArgConfig.S3Bucket, minio.ListObjectsOptions{
			Prefix:    fmt.Sprintf("%s/backups/%d/", cmd.ArgConfig.S3Folder, predate),
			Recursive: false,
		})

		for timedir := range timedirobjects {
			timedirname, err := strconv.Atoi(strings.Split(timedir.Key, "/")[3])
			if err != nil {
				cmd.LogError("The s3 contains unknown files: %s", timedir.Key)
				os.Exit(1)
			}
			timedirs = append(timedirs, timedirname)
		}

		// 找出大于之前还原时间最小的时间戳
		mintime := findMinGreaterThan(pretime, timedirs)
		if mintime != -1 {
			RestoreDate = fmt.Sprintf("%d", predate)
			RestoreTime = fmt.Sprintf("%d", mintime)
		} else {
			// 列出备份目录下的日期
			var datedirs []int
			datedirobjects := s3client.ListObjects(ctx, cmd.ArgConfig.S3Bucket, minio.ListObjectsOptions{
				Prefix:    fmt.Sprintf("%s/backups/", cmd.ArgConfig.S3Folder),
				Recursive: false,
			})

			for datedir := range datedirobjects {
				datedirname, err := strconv.Atoi(strings.Split(datedir.Key, "/")[2])
				if err != nil {
					cmd.LogError("The s3 contains unknown files: %s", datedir.Key)
					os.Exit(1)
				}
				datedirs = append(datedirs, datedirname)
			}
			// 找出大于之前还原日期最小的日期
			mindate := findMinGreaterThan(predate, datedirs)
			if mindate != -1 {
				timedirs = nil
				timedirobjects := s3client.ListObjects(ctx, cmd.ArgConfig.S3Bucket, minio.ListObjectsOptions{
					Prefix:    fmt.Sprintf("%s/backups/%d/", cmd.ArgConfig.S3Folder, mindate),
					Recursive: false,
				})

				for timedir := range timedirobjects {
					timedirname, err := strconv.Atoi(strings.Split(timedir.Key, "/")[3])
					if err != nil {
						cmd.LogError("The s3 contains unknown files: %s", timedir.Key)
						os.Exit(1)
					}
					timedirs = append(timedirs, timedirname)
				}
				mintime := findMinGreaterThan(0, timedirs)
				if mintime != -1 {
					RestoreDate = fmt.Sprintf("%d", mindate)
					RestoreTime = fmt.Sprintf("%d", mintime)
				} else {
					cmd.LogInfo("No backup found")
					return false
				}
			} else {
				cmd.LogInfo("No backup found")
				return false
			}
		}
	} else {
		var datedirs []int
		datedirobjects := s3client.ListObjects(ctx, cmd.ArgConfig.S3Bucket, minio.ListObjectsOptions{
			Prefix:    fmt.Sprintf("%s/backups/", cmd.ArgConfig.S3Folder),
			Recursive: false,
		})

		for datedir := range datedirobjects {
			datedirname, err := strconv.Atoi(strings.Split(datedir.Key, "/")[2])
			if err != nil {
				cmd.LogError("The s3 contains unknown files: %s", datedir.Key)
				os.Exit(1)
			}
			datedirs = append(datedirs, datedirname)
		}

		mindate := findMinGreaterThan(0, datedirs)
		if mindate != -1 {
			var timedirs []int
			timedirobjects := s3client.ListObjects(ctx, cmd.ArgConfig.S3Bucket, minio.ListObjectsOptions{
				Prefix:    fmt.Sprintf("%s/backups/%d/", cmd.ArgConfig.S3Folder, mindate),
				Recursive: false,
			})

			for timedir := range timedirobjects {
				timedirname, err := strconv.Atoi(strings.Split(timedir.Key, "/")[3])
				if err != nil {
					cmd.LogError("The s3 contains unknown files: %s", timedir.Key)
					os.Exit(1)
				}
				timedirs = append(timedirs, timedirname)
			}
			mintime := findMinGreaterThan(0, timedirs)
			if mintime != -1 {
				RestoreDate = fmt.Sprintf("%d", mindate)
				RestoreTime = fmt.Sprintf("%d", mintime)
			} else {
				cmd.LogInfo("No backup found")
				return false
			}
		} else {
			cmd.LogInfo("No backup found")
			return false
		}
	}

	// 获取报告, 确保要恢复的备份任务状态是success的
	metafile := fmt.Sprintf("%s/backups/%s/%s/gpdbbr_%s_jobinfo.yaml", cmd.ArgConfig.S3Folder, RestoreDate, RestoreTime, RestoreTime)
	cmd.LogInfo("Metafile = %s", metafile)
	backup_metadata_object, err := s3client.GetObject(ctx, cmd.ArgConfig.S3Bucket, metafile, minio.GetObjectOptions{})
	if err != nil {
		cmd.LogError("Failed to read backup metadata file: %s", err.Error())
		os.Exit(1)
	}
	defer backup_metadata_object.Close()

	backup_metadata, err := ioutil.ReadAll(backup_metadata_object)
	if err != nil {
		cmd.LogError("Failed to read backup metadata file: %s", err.Error())
		os.Exit(1)
	}

	if err := yaml.Unmarshal(backup_metadata, &BkYaml); err != nil {
		cmd.LogError("Failed to read backup metadata file: %s", err.Error())
		os.Exit(1)
	}

	// 判断dbname
	if BkYaml.JobInfo.DBName != cmd.ArgConfig.DbName {
		cmd.LogError("Metafile dbname is %s not equal to the dbname in the command line arguments", BkYaml.JobInfo.DBName)
		os.Exit(1)
	}

	// 判断任务状态
	if BkYaml.JobInfo.Status == "warning" {
		cmd.LogError("Restore backup task status is warning")
		os.Exit(1)
	}

	cmd.LogInfo("Restore Key = %s", RestoreTime)
	return true
}

func commrestore() {
	dbconn := cmd.CreateDbConn(cmd.ArgConfig.DbName)
	defer dbconn.Close()

	// 获取数据库的版本号
	var dbversion string
	if err := dbconn.QueryRow("select version()").Scan(&dbversion); err != nil {
		cmd.LogError("Failed to get Database Version: %s", err.Error())
		os.Exit(1)
	}

	dbvbegin := strings.Index(dbversion, "Greenplum Database ")
	dbvend := strings.Index(dbversion[dbvbegin:], ")")
	dbversion = dbversion[dbvbegin+len("Greenplum Database ") : dbvbegin+dbvend]
	cmd.LogInfo("Greenplum Database Version = %s", dbversion)

	cmd.LogInfo("Checking basic restore environment")

	// 校验用户是否存在
	usertext := "'"
	for _, user := range BkYaml.UserList {
		usertext = usertext + user + "','"
	}
	usertext = usertext[:len(usertext)-2]
	var usercnt int
	if err := dbconn.QueryRow(fmt.Sprintf("select count(*) from pg_catalog.pg_user where usename in (%s)", usertext)).Scan(&usercnt); err != nil {
		cmd.LogError("Failed to check user: %s", err.Error())
		os.Exit(1)
	}

	if usercnt != len(BkYaml.UserList) {
		cmd.LogError("User %s not exists", usertext)
		os.Exit(1)
	}

	// 全量还原, 必须是空库
	if RestoreType == "full" {
		tabcnt := 0
		if err := dbconn.QueryRow("select count(*) from gp_toolkit.__gp_user_tables").Scan(&tabcnt); err != nil {
			cmd.LogError("Failed to check table count: %s", err.Error())
			os.Exit(1)
		}
		if tabcnt != 0 {
			cmd.LogError("Full restore must be empty database")
			os.Exit(1)
		}
	}

	RestoreRpt.BeginTime = time.Now().Format("20060102150405000")

	// 开始恢复
	if RestoreType == "increment" {
		if len(BkYaml.DataEntries) == 0 && len(BkYaml.DdlSqls) == 0 {
			cmd.LogInfo("No table data need to restore")
		}
		cmd.LogInfo("Droping incremental restore table")

		for _, table := range BkYaml.DataEntries {
			if _, err := dbconn.Exec(fmt.Sprintf("drop table if exists %s cascade", table.TableName)); err != nil {
				cmd.LogError("Drop table %s failed: %s", table.TableName, err.Error())
				os.Exit(1)
			}
		}

		// 也要删除没有分区的父表
		getnullpnamesql := `
		SELECT pnp.nspname||'.'||parent.relname AS pname
		FROM pg_partitioned_table pt 
		JOIN pg_class parent ON pt.partrelid = parent.oid
		JOIN pg_namespace pnp on parent.relnamespace = pnp.oid
		LEFT JOIN pg_inherits i ON i.inhparent = parent.oid
		WHERE i.inhrelid IS NULL;
		`

		rows, err := dbconn.Query(getnullpnamesql)
		if err != nil {
			cmd.LogError("Failed to get parent table name: %s", err.Error())
			os.Exit(1)
		}
		defer rows.Close()

		for rows.Next() {
			var tabname string
			err = rows.Scan(&tabname)
			if err != nil {
				cmd.LogError("Failed to get parent table name: %s", err.Error())
				os.Exit(1)
			}

			if _, err := dbconn.Exec(fmt.Sprintf("drop table if exists %s cascade", tabname)); err != nil {
				cmd.LogError("Drop table %s failed: %s", tabname, err.Error())
				os.Exit(1)
			}
		}
	}

	// 下发s3配置文件
	cmd.LogInfo("Distributing S3 configuration file to all hosts")
	GpHome = os.Getenv("GPHOME")
	if GpHome == "" {
		cmd.LogError("GPHOME is not set")
		os.Exit(1)
	}
	rows, err := dbconn.Query("select distinct hostname from gp_segment_configuration where role = 'p' and content <> '-1'")
	if err != nil {
		cmd.LogError("Failed to get host lists: %s", err.Error())
		os.Exit(1)
	}

	for rows.Next() {
		var host string
		err = rows.Scan(&host)
		if err != nil {
			cmd.LogError("Failed to get host lists: %s", err.Error())
			os.Exit(1)
		}
		HostList = append(HostList, host)
	}

	cmd.CreS3Yaml(RestoreTime, GpHome, HostList)

	// 执行表结构恢复
	cmd.LogInfo("Restoring pre-data metadata")

	// 获取表结构文件
	s3client := cmd.CreS3Client()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var metafile string
	if RestoreType == "increment" {
		metafile = fmt.Sprintf("%s/backups/%s/%s/gpdbbr_%s_incr_metadata.sql", cmd.ArgConfig.S3Folder, RestoreDate, RestoreTime, RestoreTime)
	} else {
		metafile = fmt.Sprintf("%s/backups/%s/%s/gpdbbr_%s_all_metadata.sql", cmd.ArgConfig.S3Folder, RestoreDate, RestoreTime, RestoreTime)
	}

	// 判断是否有该文件
	_, err = s3client.StatObject(ctx, cmd.ArgConfig.S3Bucket, metafile, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			cmd.LogInfo("no data need to restore")
			return
		} else {
			cmd.LogError("Failed to get backup metadata file: %s", err.Error())
			os.Exit(1)
		}
	}

	cmd.LogInfo("Reading backup metadata file: %s", metafile)
	backup_metadata_object, err := s3client.GetObject(ctx, cmd.ArgConfig.S3Bucket, metafile, minio.GetObjectOptions{})
	if err != nil {
		cmd.LogError("Failed to read backup metadata file: %s", err.Error())
		os.Exit(1)
	}
	defer backup_metadata_object.Close()

	sqlbinary, err := ioutil.ReadAll(backup_metadata_object)
	if err != nil {
		cmd.LogError("Failed to read backup metadata file: %s", err.Error())
		os.Exit(1)
	}

	sqlscript := string(sqlbinary)
	_, err = dbconn.Exec(sqlscript)
	if err != nil {
		cmd.LogError("Failed to execute backup metadata file sql scripts: %s", err.Error())
		os.Exit(1)
	}

	cmd.LogInfo("Pre-data metadata restore complete")
	cmd.LogInfo("Restoring table data")

	tabchan := make(chan cmd.DataEntry, 1000000)
	for _, table := range BkYaml.DataEntries {
		tabchan <- table
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < cmd.ArgConfig.Jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dbconn1 := cmd.CreateDbConn(cmd.ArgConfig.DbName)
			defer dbconn1.Close()
			for table := range tabchan {
				isrs := restoredata(dbconn1, table.TableName, table.AttributeString, table.OID)
				if !isrs {
					mu.Lock()
					RestoreRpt.FailTables = append(RestoreRpt.FailTables, table.TableName)
					mu.Unlock()
				}
			}
		}()
	}
	close(tabchan)
	wg.Wait()
	cmd.LogInfo("Data restore complete")

	// 执行增量ddl恢复
	if RestoreType == "increment" && len(BkYaml.DdlSqls) > 0 {
		cmd.LogInfo("Restoring incremental ddl sql")
		ddlchan := make(chan string, 1000000)
		for _, ddl := range BkYaml.DdlSqls {
			ddlchan <- ddl
		}

		for i := 0; i < cmd.ArgConfig.Jobs; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dbconn1 := cmd.CreateDbConn(cmd.ArgConfig.DbName)
				defer dbconn1.Close()
				for ddl := range ddlchan {
					_, err = dbconn1.Exec(ddl)
					if err != nil {
						cmd.LogInfo("Failed to execute ddl, %s : %s", ddl, err.Error())
						mu.Lock()
						RestoreRpt.FailDDLs = append(RestoreRpt.FailDDLs, ddl)
						mu.Unlock()
					}
				}
			}()
		}
		close(ddlchan)
		wg.Wait()
		cmd.LogInfo("Incremental ddl restore complete")
	}
}
