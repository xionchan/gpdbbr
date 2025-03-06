package rowchk

import (
	"context"
	"fmt"
	"gpdbbr/cmd"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"gopkg.in/yaml.v3"
)

func DoRowChk() {
	isdo := getbaseinfo()

	if isdo {
		docheck()
		if len(TabCk.OnlyBk) == 0 && len(TabCk.OnlyDb) == 0 && len(TabCk.DiffRow) == 0 {
			cmd.LogInfo("Row check success")
		} else {
			// 写入报告
			rowchkrpt := fmt.Sprintf("%s/rowcheck_%d_report", RptDir, CkTime)
			yamldata, err := yaml.Marshal(&TabCk)
			if err != nil {
				cmd.LogError("Failed to marshal rowchk report: %s", err.Error())
				os.Exit(1)
			}
			err = os.WriteFile(rowchkrpt, yamldata, 0644)
			if err != nil {
				cmd.LogError("Failed to write rowchk report: %s", err.Error())
				os.Exit(1)
			}
			cmd.LogInfo("Restore report: %s", rowchkrpt)
			cmd.LogInfo("Row check complete, but some table has problem")
		}
	}
}

func getbaseinfo() bool {
	cmd.LogInfo("Geting restore infomation")

	CnDir = os.Getenv("COORDINATOR_DATA_DIRECTORY")
	if CnDir == "" {
		cmd.LogError("COORDINATOR_DATA_DIRECTORY environment variable not set")
		os.Exit(1)
	}

	ckdate := 0
	CkTime = 0
	ckdir := fmt.Sprintf("%s/gpdbbr/%s", CnDir, cmd.ArgConfig.DbName)
	_, err := os.Stat(ckdir)
	if err != nil {
		if os.IsNotExist(err) {
			cmd.LogInfo("Restore info directory not found, skip rowchk")
			return false
		} else {
			cmd.LogError("Failed to get Restore info directory: %s", err.Error())
			os.Exit(1)
		}
	} else {
		// 校验还原目录
		pattern := filepath.Join(ckdir, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")
		matches, err := filepath.Glob(pattern)
		if err != nil {
			cmd.LogError("Failed to get Restore info directory: %s", err.Error())
			os.Exit(1)
		}

		if len(matches) == 0 {
			cmd.LogInfo("Restore info directory not found, skip rowchk")
			return false
		} else {
			sort.Sort(sort.Reverse(sort.StringSlice(matches)))
			ckdate, _ = strconv.Atoi(filepath.Base(matches[0]))

			// 获取timestamp
			datedir := fmt.Sprintf("%s/%d", ckdir, ckdate)
			pattern = filepath.Join(datedir, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")
			matches, err = filepath.Glob(pattern)
			if err != nil {
				cmd.LogError("Failed to get Restore info directory: %s", err.Error())
				os.Exit(1)
			}

			if len(matches) == 0 {
				cmd.LogInfo("Restore info directory not found, skip rowchk")
				return false
			} else {
				sort.Sort(sort.Reverse(sort.StringSlice(matches)))
				CkTime, _ = strconv.Atoi(filepath.Base(matches[0]))
				rptfile := fmt.Sprintf("%s/%d/gpdbbr_%d_report", datedir, CkTime, CkTime)
				cmd.LogInfo("Report file = %s", rptfile)
				cmd.LogInfo("Check Key = %d", CkTime)
				RptDir = fmt.Sprintf("%s/%d", datedir, CkTime)

				// 解析文件
				var rpt cmd.RsRpt
				data, err := os.ReadFile(rptfile)
				if err != nil {
					cmd.LogError("Failed to read report file: %s", err.Error())
					os.Exit(1)
				}

				err = yaml.Unmarshal(data, &rpt)
				if err != nil {
					cmd.LogError("Failed to parse report file: %s", err.Error())
					os.Exit(1)
				}

				if rpt.Status != "success" {
					cmd.LogError("Backup failed, skip rowchk")
					os.Exit(1)
				}
			}
		}

	}

	// s3校验
	s3client := cmd.CreS3Client()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metafile := fmt.Sprintf("%s/backups/%d/%d/gpdbbr_%d_jobinfo.yaml", cmd.ArgConfig.S3Folder, ckdate, CkTime, CkTime)
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

	// 解析文件
	if err := yaml.Unmarshal(backup_metadata, &BkMeta); err != nil {
		cmd.LogError("Failed to read backup metadata file: %s", err.Error())
		os.Exit(1)
	}

	// 判断dbname
	if BkMeta.JobInfo.DBName != cmd.ArgConfig.DbName {
		cmd.LogError("Metafile dbname is %s not equal to the dbname in the command line arguments", BkMeta.JobInfo.DBName)
		os.Exit(1)
	}

	// 判断任务状态
	if BkMeta.JobInfo.Status == "warning" {
		cmd.LogError("Backup task status is warning, skip rowchk")
		os.Exit(1)
	}

	return true
}

func docheck() {
	dbconn := cmd.CreateDbConn(cmd.ArgConfig.DbName)
	defer dbconn.Close()

	// 确保已经收集完统计信息
	for {
		var isdone int
		err := dbconn.QueryRow("select count(*) from pg_stat_progress_analyze").Scan(&isdone)
		if err != nil {
			cmd.LogError("Failed to query pg_stat_progress_analyze: %s", err.Error())
			os.Exit(1)
		}
		if isdone == 0 {
			time.Sleep(3 * time.Second)
			err = dbconn.QueryRow("select count(*) from pg_stat_progress_analyze").Scan(&isdone)
			if err != nil {
				cmd.LogError("Failed to query pg_stat_progress_analyze: %s", err.Error())
				os.Exit(1)
			}
			if isdone == 0 {
				break
			}
		}
		time.Sleep(10 * time.Second)
	}

	rows, err := dbconn.Query("select schemaname||'.'||relname as tabname, n_live_tup as tabrow from pg_stat_all_tables where schemaname not in ('logddl', 'information_schema') and schemaname not like 'pg%'")
	if err != nil {
		cmd.LogError("Failed to query table row count: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	tabrows := make(map[string]float64)
	for rows.Next() {
		var tabname string
		var tabrow float64
		err = rows.Scan(&tabname, &tabrow)
		if err != nil {
			cmd.LogError("Failed to scan table row count: %s", err.Error())
			os.Exit(1)
		}
		tabrows[tabname] = tabrow
	}

	onlya, onlyb, diff := checkdata(BkMeta.TableRows, tabrows)

	if len(onlya) > 0 {
		for _, table := range onlya {
			TabCk.OnlyBk = append(TabCk.OnlyBk, table)
			cmd.LogError("Table row count check failed, only in backup: %v", table)
		}
	}

	if len(onlyb) > 0 {
		for _, table := range onlyb {
			TabCk.OnlyDb = append(TabCk.OnlyDb, table)
			cmd.LogError("Table row count check failed, only in database: %v", table)
		}
	}

	if len(diff) > 0 {
		for _, diftab := range diff {
			if diftab.BkRow == 0 || diftab.DbRow == 0 {
				TabCk.DiffRow = append(TabCk.DiffRow, diftab)
				cmd.LogError("Table row count check failed, diff in backup and database: %v, bk: %0.f, db: %0.f", diftab.TabName, diftab.BkRow, diftab.DbRow)
			} else if math.Abs(diftab.BkRow-diftab.DbRow)/diftab.BkRow > 0.05 && math.Abs(diftab.BkRow-diftab.DbRow) > 300 {
				TabCk.DiffRow = append(TabCk.DiffRow, diftab)
				cmd.LogError("Table row count check failed, diff in backup and database: %v, bk: %0.f, db: %0.f", diftab.TabName, diftab.BkRow, diftab.DbRow)
			}
		}
	}
}
