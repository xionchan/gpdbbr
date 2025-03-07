package backup

import (
	"context"
	"fmt"
	"gpdbbr/cmd"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/minio/minio-go/v7"
)

func DoBackup() {
	// 判断备份类型
	getbktype()
	if BackupType == "full" {
		cmd.LogInfo("Backup type = full backup")
	} else {
		cmd.LogInfo("Backup type = incremental backup")
	}

	// 执行备份
	commbackup()

	// 写入状态
	BkResult.JobInfo.DBName = cmd.ArgConfig.DbName
	BkResult.JobInfo.BeginTime = Timestamp
	if len(BkResult.FailTables) > 0 {
		BkResult.JobInfo.Status = "warning"
	} else {
		BkResult.JobInfo.Status = "success"
	}

	BkResult.JobInfo.EndTime = time.Now().Format("20060102150405000")

	yamldata, err := yaml.Marshal(BkResult)
	if err != nil {
		cmd.LogError("Failed to marshal backup result to yaml: %s", err.Error())
		os.Exit(1)
	}

	err = os.WriteFile(fmt.Sprintf("/tmp/bkresult_%s.yaml", Timestamp), yamldata, 0644)
	if err != nil {
		cmd.LogError("Failed to write backup result to file: %s", err.Error())
		os.Exit(1)
	}

	cmd.LogInfo("Write backup job information to %s/backups/%s/%s/gpdbbr_%s_jobinfo.yaml", cmd.ArgConfig.S3Folder, BackupDate, Timestamp, Timestamp)
	cmd.PutFileToS3(fmt.Sprintf("/tmp/bkresult_%s.yaml", Timestamp), fmt.Sprintf("%s/backups/%s/%s/gpdbbr_%s_jobinfo.yaml", cmd.ArgConfig.S3Folder, BackupDate, Timestamp, Timestamp))

	if BkResult.JobInfo.Status == "success" {
		cmd.LogInfo("Backup completed successfully")
	} else {
		cmd.LogInfo("Backup completed with failed tables")
	}
}

func getbktype() {
	cmd.LogInfo("Checking backup type")
	// 创建minio客户端
	s3client := cmd.CreS3Client()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 列出日期目录
	var datedirs []int
	datedirobjects := s3client.ListObjects(ctx, cmd.ArgConfig.S3Bucket, minio.ListObjectsOptions{
		Prefix:    fmt.Sprintf("%s/backups/", cmd.ArgConfig.S3Folder),
		Recursive: false,
	})

	for datedir := range datedirobjects {
		if datedir.Err != nil {
			BackupType = "full"
			return
		}
		datedirname, err := strconv.Atoi(strings.Split(datedir.Key, "/")[2])
		if err != nil {
			cmd.LogError("The s3 contains unknown files: %s", datedir.Key)
			os.Exit(1)
		}
		datedirs = append(datedirs, datedirname)
	}

	// 如果没有日期目录，那么表示全量备份
	if len(datedirs) == 0 {
		BackupType = "full"
		return
	}

	// 判断增量备份格式是否正常
	// 取出最大日期
	maxdate := datedirs[0]
	for _, datenum := range datedirs {
		if datenum > maxdate {
			maxdate = datenum
		}
	}

	// 取出最大时间
	var timedirs []int
	timedirobjects := s3client.ListObjects(ctx, cmd.ArgConfig.S3Bucket, minio.ListObjectsOptions{
		Prefix:    fmt.Sprintf("%s/backups/%d/", cmd.ArgConfig.S3Folder, maxdate),
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

	maxtime := timedirs[0]
	for _, timenum := range timedirs {
		if timenum > maxtime {
			maxtime = timenum
		}
	}

	// 校验gpdbbr源数据文件
	metafile := fmt.Sprintf("%s/backups/%d/%d/gpdbbr_%d_jobinfo.yaml", cmd.ArgConfig.S3Folder, maxdate, maxtime, maxtime)
	backup_metadata_object, err := s3client.GetObject(ctx, cmd.ArgConfig.S3Bucket, metafile, minio.GetObjectOptions{})
	if err != nil {
		cmd.LogError("Failed to get backup metadata file(%s): %s", metafile, err.Error())
		os.Exit(1)
	}
	defer backup_metadata_object.Close()

	backup_metadata, err := ioutil.ReadAll(backup_metadata_object)
	if err != nil {
		cmd.LogError("Failed to read backup metadata file(%s): %s", metafile, err.Error())
		os.Exit(1)
	}

	if err := yaml.Unmarshal(backup_metadata, &IncrYaml); err != nil {
		cmd.LogError("Failed to unmarshal backup metadata file(%s): %s", metafile, err.Error())
		os.Exit(1)
	}

	// 判断dbname是否和传入一直
	if IncrYaml.JobInfo.DBName != cmd.ArgConfig.DbName {
		cmd.LogError("Metafile dbname(%s) not equal to the dbname in the command line arguments", IncrYaml.JobInfo.DBName)
		os.Exit(1)
	}

	// 判断任务状态
	if IncrYaml.JobInfo.Status == "warning" {
		cmd.LogError("Previous backup job status is warning")
		os.Exit(1)
	}

	BackupType = "increment"
}

func commbackup() {
	// 连接数据库
	dbconn := cmd.CreateDbConn(cmd.ArgConfig.DbName)
	defer dbconn.Close()

	// 获取数据库的版本号
	var dbversion string
	err := dbconn.QueryRow("select version()").Scan(&dbversion)
	if err != nil {
		cmd.LogError("Failed to get Database Version: %s", err.Error())
		os.Exit(1)
	}

	dbvbegin := strings.Index(dbversion, "Greenplum Database ")
	dbvend := strings.Index(dbversion[dbvbegin:], ")")
	dbversion = dbversion[dbvbegin+len("Greenplum Database ") : dbvbegin+dbvend]
	cmd.LogInfo("Greenplum Database Version = %s", dbversion)
	cmd.LogInfo("Starting backup of database %s", cmd.ArgConfig.DbName)
	cmd.LogInfo("Gathering backup base infomation")

	// 获取DBOID
	err = dbconn.QueryRow(fmt.Sprintf("select oid from pg_database where datname = '%s'", cmd.ArgConfig.DbName)).Scan(&DbOid)
	if err != nil {
		cmd.LogError("Failed to get Database OID: %s", err.Error())
		os.Exit(1)
	}

	// 获取catalog version number
	cndir := os.Getenv("COORDINATOR_DATA_DIRECTORY")
	if cndir == "" {
		cmd.LogError("Failed to get COORDINATOR_DATA_DIRECTORY environment variable")
		os.Exit(1)
	}

	ok, controldata := cmd.ExecOsCmd("pg_controldata", []string{"-D", cndir})
	if !ok {
		cmd.LogError("Failed to get database catalog version number: %s", controldata)
		os.Exit(1)
	}

	lines := strings.Split(controldata, "\n")
	for _, line := range lines {
		if strings.Contains(line, "Catalog version number") {
			Catavers = strings.TrimSpace(strings.Split(line, ":")[1])
			break
		}
	}

	// 获取GPHOME环境变量
	GpHome = os.Getenv("GPHOME")
	if GpHome == "" {
		cmd.LogError("Failed to get GPHOME environment variable")
		os.Exit(1)
	}

	// 做一个检查点
	_, err = dbconn.Exec("checkpoint")
	if err != nil {
		cmd.LogError("Failed to execute checkpoint: %s", err.Error())
		os.Exit(1)
	}

	// 开启事务, 设置事务隔离级别
	dbtx, err := dbconn.Begin()
	if err != nil {
		cmd.LogError("Failed to begin transaction: %s", err.Error())
		os.Exit(1)
	}
	defer func() {
		if err != nil {
			_ = dbtx.Rollback()
		} else {
			_ = dbtx.Commit()
		}
	}()

	// 初始化事务信息
	_, err = dbtx.Exec(`
	    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
		SET lock_timeout = 0;
		SET idle_in_transaction_session_timeout = 0;
		`)
	if err != nil {
		cmd.LogError("Failed to init transaction information: %s", err.Error())
		os.Exit(1)
	}

	// 获取unix时间戳, 时间戳, 事务快照ID
	err = dbtx.QueryRow(`
	SELECT FLOOR(EXTRACT(EPOCH FROM NOW()))::BIGINT AS unix_timestamp, 
	TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSMS') AS formatted_time,
	TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDD') AS formatted_date,
	pg_export_snapshot() AS snap_id;
	`).Scan(&UnixTime, &Timestamp, &BackupDate, &DbSnapShot)
	if err != nil {
		cmd.LogError("Failed to get unix timestamp, timestamp, transaction snapshot id: %s", err.Error())
		os.Exit(1)
	}
	cmd.LogInfo("Backup Timestamp = %s", Timestamp)

	// 初始化SSH会话
	cmd.LogInfo("Initializing SSH sessions")
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

	cmd.InitSess(HostList)

	// 下发s3配置文件
	cmd.LogInfo("Distributing S3 configuration file to all hosts")
	cmd.CreS3Yaml(Timestamp, GpHome, HostList)

	// lock表操作 : 无论备份类型，都锁定所有表
	cmd.LogInfo("Gathering table state information")
	getalltablename := `
	SELECT n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname)||'.'||quote_ident(c.relname) as tablename 
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE n.nspname NOT LIKE 'pg_temp_%' 
	AND n.nspname NOT LIKE 'pg_toast%' 
	AND n.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog', 'logddl')
	AND relkind IN ('r', 'p')
	AND c.oid NOT IN (select objid from pg_depend where deptype = 'e')
	ORDER BY c.oid;
	`
	rows, err = dbtx.Query(getalltablename)
	if err != nil {
		cmd.LogError("Failed to get all table name: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	alltablelist, err := cmd.ProcessRows(rows)
	if err != nil {
		cmd.LogError("Failed to get all table name: %s", err.Error())
		os.Exit(1)
	}

	// 锁表
	locksql := "LOCK TABLE "
	for _, table := range alltablelist {
		locksql = locksql + fmt.Sprintf("%v", table["tablename"]) + ", "
	}
	locksql = locksql[:len(locksql)-2] + " IN ACCESS SHARE MODE COORDINATOR ONLY"
	cmd.LogInfo("Acquiring ACCESS SHARE locks on all tables")
	_, err = dbtx.Exec(locksql)
	if err != nil {
		cmd.LogError("Failed to lock all table: %s", err.Error())
		os.Exit(1)
	}

	cmd.LogInfo("Metadata write to %s/backups/%s/%s/gpdbbr_%s_all_metadata.sql", cmd.ArgConfig.S3Folder, BackupDate, Timestamp, Timestamp)
	// 另外起一个线程，调用os命令, 导出源数据库
	var oswg sync.WaitGroup
	oswg.Add(1)
	go func() {
		defer oswg.Done()
		dumpmeta()
	}()

	cmd.LogInfo("Gathering additional table metadata")
	// 排除分区表父表
	gettablename := `
	SELECT n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname)||'.'||quote_ident(c.relname) as tablename 
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE n.nspname NOT LIKE 'pg_temp_%' 
	AND n.nspname NOT LIKE 'pg_toast%' 
	AND n.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog', 'logddl')
	AND relkind IN ('r', 'p')
	AND c.relfilenode <> 0
	AND c.oid NOT IN (select objid from pg_depend where deptype = 'e')
	ORDER BY c.oid;
	`

	rows, err = dbtx.Query(gettablename)
	if err != nil {
		cmd.LogError("Failed to get table name: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	tablelist, err := cmd.ProcessRows(rows)
	if err != nil {
		cmd.LogError("Failed to get table name: %s", err.Error())
		os.Exit(1)
	}

	// 构造列信息
	getcolname := `
	SELECT a.attrelid as oid,
	STRING_AGG(quote_ident(a.attname)::text,', ' order by a.attnum) AS colnameagg
	FROM pg_catalog.pg_attribute a 
	JOIN pg_class c ON a.attrelid = c.oid
	JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE n.nspname NOT LIKE 'pg_temp_%' 
	AND n.nspname NOT LIKE 'pg_toast%' 
	AND n.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog', 'logddl')
	AND c.reltype <> 0
	AND a.attnum > 0::pg_catalog.int2
	AND a.attisdropped = 'f'
	group by a.attrelid
	ORDER BY a.attrelid;
	`

	rows, err = dbtx.Query(getcolname)
	if err != nil {
		cmd.LogError("Failed to get table column name: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	collist, err := cmd.ProcessRows(rows)
	if err != nil {
		cmd.LogError("Failed to get table column name: %s", err.Error())
		os.Exit(1)
	}

	// 合并数据
	tabinfolist := cmd.MergeSlices(tablelist, collist, "oid")

	// 获取lastddltime
	getddlsql := `
	SELECT quote_ident(aoschema) || '.' || quote_ident(aorelname) as tablename,
	TO_CHAR(lastddltimestamp, 'YYYYMMDDHH24MISSMS') as lastddltimestamp
	FROM ( SELECT c.oid AS aooid,
	n.nspname AS aoschema,
	c.relname AS aorelname
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
	JOIN pg_am a ON c.relam = a.oid
	WHERE a.amname in ('ao_row', 'ao_column')
	AND n.nspname NOT LIKE 'pg_temp_%' 
	AND n.nspname NOT LIKE 'pg_toast%' 
	AND n.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog', 'logddl')) aotables
	JOIN ( SELECT lo.objid,
	MAX(lo.statime) AS lastddltimestamp
	FROM pg_stat_last_operation lo
	WHERE lo.staactionname IN ('CREATE', 'ALTER', 'TRUNCATE')
	GROUP BY lo.objid) lastop
	ON aotables.aooid = lastop.objid;
	`

	rows, err = dbtx.Query(getddlsql)
	if err != nil {
		cmd.LogError("Failed to get ao table lastddltime: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	aoddllist, err := cmd.ProcessRows(rows)
	if err != nil {
		cmd.LogError("Failed to get ao table lastddltime: %s", err.Error())
		os.Exit(1)
	}

	// 合并数据
	tabinfolist = cmd.MergeSlices(tabinfolist, aoddllist, "tablename")

	// 获取ao表的aosegtablefqn
	getaofqnsql := `
	SELECT seg.aotablefqn as tablename,
	'pg_aoseg.' || quote_ident(aoseg_c.relname) AS aosegtablefqn
    FROM pg_class aoseg_c
    JOIN (SELECT pg_ao.relid AS aooid,
	pg_ao.segrelid,
	aotables.aotablefqn
	FROM pg_appendonly pg_ao
	JOIN (SELECT c.oid,
	quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS aotablefqn
	FROM pg_class c
	JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_am a ON c.relam = a.oid
    WHERE a.amname in ('ao_row', 'ao_column')
    AND n.nspname NOT LIKE 'pg_temp_%' 
	AND n.nspname NOT LIKE 'pg_toast%' 
	AND n.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog', 'logddl')) aotables 
	ON pg_ao.relid = aotables.oid) seg 
	ON aoseg_c.oid = seg.segrelid;
	`

	rows, err = dbtx.Query(getaofqnsql)
	if err != nil {
		cmd.LogError("Failed to get ao table aosegtablefqn: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	aofqnlist, err := cmd.ProcessRows(rows)
	if err != nil {
		cmd.LogError("Failed to get ao table aosegtablefqn failed: %s", err.Error())
		os.Exit(1)
	}

	// 合并数据
	tabinfolist = cmd.MergeSlices(tabinfolist, aofqnlist, "tablename")

	// 获取ao表的ddl语句
	if BackupType != "full" {
		getaoddl := fmt.Sprintf(`
		with rank_ddl as(
        select timestamp,
        object_name as tablename,
        ddl_query,
        ROW_NUMBER() over (
        partition by object_name
        order by timestamp desc) as rn
        from logddl.ddl_log
        where ddl_type = 'ALTER TABLE'
		and timestamp < to_timestamp('%s', 'YYYYMMDDHH24MISSMS')
        and ddl_query ~* 'set\s+(tablespace|with)'
        and object_name not in (
        select object_name
        from logddl.ddl_log           
        where ddl_query !~* 'set\s+(tablespace|with)'))
		select timestamp ,
        tablename,
        ddl_query
		from rank_ddl
		where rn = 1;
		`, Timestamp)

		rows, err = dbtx.Query(getaoddl)
		if err != nil {
			cmd.LogError("Failed get ao table ddl: %s", err.Error())
			os.Exit(1)
		}
		defer rows.Close()

		aoddlsqllist, err := cmd.ProcessRows(rows)
		if err != nil {
			cmd.LogError("Failed get ao table ddl: %s", err.Error())
			os.Exit(1)
		}

		// 合并数据
		tabinfolist = cmd.MergeSlices(tabinfolist, aoddlsqllist, "tablename")
	}

	// 放入通道
	dotablelist := make(chan map[string]interface{}, 1000000)
	for _, row := range tabinfolist {
		dotablelist <- row
	}

	cmd.LogInfo("Writing table data to s3 file")
	var wg2 sync.WaitGroup
	var mu sync.Mutex
	BkResult.IncrementalMetadata.AO = make(map[string]cmd.AoMetadata)
	BkResult.IncrementalMetadata.Heap = make(map[string]cmd.HeapMetadata)
	for i := 0; i < cmd.ArgConfig.Jobs; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			failtab, incrinfo, dataent, ddls := workthread(dotablelist)

			mu.Lock()
			for _, v := range failtab {
				BkResult.FailTables = append(BkResult.FailTables, v)
			}
			for k, v := range incrinfo.AO {
				BkResult.IncrementalMetadata.AO[k] = v
			}
			for k, v := range incrinfo.Heap {
				BkResult.IncrementalMetadata.Heap[k] = v
			}
			for _, v := range dataent {
				BkResult.DataEntries = append(BkResult.DataEntries, v)
			}
			for _, v := range ddls {
				BkResult.DdlSqls = append(BkResult.DdlSqls, v)
			}
			mu.Unlock()
		}()
	}

	// 要等待子进程结束
	close(dotablelist)
	oswg.Wait()
	wg2.Wait()

	// 备份增量表DDL
	if BackupType == "increment" {
		var tablist []string
		if BkResult.DataEntries != nil {
			tabsqlstr := "'" // 用于过滤分区表
			for _, v := range BkResult.DataEntries {
				tablist = append(tablist, v.TableName)
				tabsqlstr += v.TableName + "','"
			}
			// 所有子分区都增量备份了, 那么父分区也需要增量备份
			tabsqlstr = tabsqlstr[:len(tabsqlstr)-2]
			getpnamesql := fmt.Sprintf(`
			with gppart as (
			select schemaname||'.'||tablename as fname, 
			partitionschemaname||'.'||partitiontablename as pname 
			from gp_toolkit.gp_partitions)
			select fname from gppart 
			group by fname 
			having count(*) = 
			count(case when pname in (%s) then 1 end);
			`, tabsqlstr)

			rows, err = dbconn.Query(getpnamesql)
			if err != nil {
				cmd.LogError("Failed to get parent table name: %s", err.Error())
				os.Exit(1)
			}
			defer rows.Close()

			for rows.Next() {
				var parentname string
				err = rows.Scan(&parentname)
				if err != nil {
					cmd.LogError("Failed to get parent table name: %s", err.Error())
					os.Exit(1)
				}
				tablist = append(tablist, parentname)
			}
		}

		// 只有父表, 没有子表, 那么父表也需要增量备份
		getnullpnamesql := `
		SELECT pnp.nspname||'.'||parent.relname AS pname
		FROM pg_partitioned_table pt 
		JOIN pg_class parent ON pt.partrelid = parent.oid
		JOIN pg_namespace pnp on parent.relnamespace = pnp.oid
		LEFT JOIN pg_inherits i ON i.inhparent = parent.oid
		WHERE i.inhrelid IS NULL;
		`

		rows, err = dbconn.Query(getnullpnamesql)
		if err != nil {
			cmd.LogError("Failed to get parent table name: %s", err.Error())
			os.Exit(1)
		}
		defer rows.Close()

		for rows.Next() {
			var parentname string
			err = rows.Scan(&parentname)
			if err != nil {
				cmd.LogError("Failed to get parent table name: %s", err.Error())
				os.Exit(1)
			}
			tablist = append(tablist, parentname)
		}

		if len(tablist) > 0 {
			cmd.LogInfo("Getting incremental metadata to %s/backups/%s/%s/gpdbbr_%s_incr_metadata.sql", cmd.ArgConfig.S3Folder, BackupDate, Timestamp, Timestamp)
			dumptabddl(tablist)
		} else {
			cmd.LogInfo("No table need to backup")
		}
	}

	// 获取用户列表
	getusersql := "select distinct pg_catalog.pg_get_userbyid(relowner) from pg_class"
	rows, err = dbconn.Query(getusersql)
	if err != nil {
		cmd.LogError("Failed to get users: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	for rows.Next() {
		var username string
		err = rows.Scan(&username)
		if err != nil {
			cmd.LogError("Failed to get users failed: %s", err.Error())
			os.Exit(1)
		}
		BkResult.UserList = append(BkResult.UserList, username)
	}

	// 清理ddl日志表数据
	cmd.LogInfo("Cleaning up ddl log table data")
	purgeddllog := fmt.Sprintf("delete from logddl.ddl_log where timestamp < to_timestamp('%s', 'YYYYMMDDHH24MISSMS')", Timestamp)
	_, err = dbconn.Exec(purgeddllog)
	if err != nil {
		cmd.LogError("Failed to Clean ddl log table: %s", err.Error())
		os.Exit(1)
	}

	// 获取表的行统计信息
	cmd.LogInfo("Getting table row statistics")
	rows, err = dbconn.Query("select schemaname||'.'||relname as tabname, n_live_tup as tabrow from pg_stat_all_tables where schemaname not in ('logddl', 'information_schema') and schemaname not like 'pg%'")
	if err != nil {
		cmd.LogError("Failed to get table row statistics: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	BkResult.TableRows = make(map[string]float64)
	for rows.Next() {
		var tabname string
		var tabrow float64
		err = rows.Scan(&tabname, &tabrow)
		if err != nil {
			cmd.LogError("Failed to get table row statistics: %s", err.Error())
			os.Exit(1)
		}
		BkResult.TableRows[tabname] = tabrow
	}
}

func workthread(dotablelist chan map[string]interface{}) ([]string, cmd.IncrementalMetadata, []cmd.DataEntry, []string) {
	dbconn := cmd.CreateDbConn(cmd.ArgConfig.DbName)
	defer dbconn.Close()

	var failtab []string
	var incrinfo cmd.IncrementalMetadata
	var dataent []cmd.DataEntry
	var ddls []string
	incrinfo.AO = make(map[string]cmd.AoMetadata)
	incrinfo.Heap = make(map[string]cmd.HeapMetadata)

	for table := range dotablelist {
		if table["aosegtablefqn"] != nil {
			modcnt, lastsegddl, isbk, err := bkaotabl(dbconn, table)
			if err != nil {
				failtab = append(failtab, table["tablename"].(string))
				cmd.LogError("Backup AO table %s failed: %s", table["tablename"].(string), err.Error())
			} else {
				incrinfo.AO[table["tablename"].(string)] = cmd.AoMetadata{
					ModCount:    modcnt,
					LastDDLTime: table["lastddltimestamp"].(string),
				}
				if isbk {
					dataent = append(dataent, cmd.DataEntry{
						TableName:       table["tablename"].(string),
						OID:             table["oid"].(string),
						AttributeString: table["colnameagg"].(string),
					})
				} else {
					if lastsegddl != "" {
						ddls = append(ddls, lastsegddl)
					}
				}
			}
		} else {
			maxstat, isbk, err := bkheaptable(dbconn, table)
			if err != nil {
				failtab = append(failtab, table["tablename"].(string))
				cmd.LogError("Backup heap table %s failed: %s", table["tablename"].(string), err.Error())
			} else {
				incrinfo.Heap[table["tablename"].(string)] = cmd.HeapMetadata{
					MaxStat: maxstat,
				}
				if isbk {
					dataent = append(dataent, cmd.DataEntry{
						TableName:       table["tablename"].(string),
						OID:             table["oid"].(string),
						AttributeString: table["colnameagg"].(string),
					})
				}
			}
		}
	}
	return failtab, incrinfo, dataent, ddls
}
