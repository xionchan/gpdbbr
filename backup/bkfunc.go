package backup

import (
	"database/sql"
	"fmt"
	"gpdbbr/cmd"
	"os"
	"strconv"
	"time"
)

func dumpmeta() {
	ok, output := cmd.ExecOsCmd("pg_dump", []string{"-s", fmt.Sprintf("--snapshot=%s", DbSnapShot), cmd.ArgConfig.DbName, "-f", fmt.Sprintf("/tmp/gpdbbr_%s_all_metadata.sql", Timestamp)})
	if !ok {
		cmd.LogError("Dump metadata fail: %s\n", output)
		os.Exit(1)
	}

	// 需要单独导出函数和存储过程的源数据
	dbconn := cmd.CreateDbConn(cmd.ArgConfig.DbName)
	defer dbconn.Close()
	// 开启事务, 设置事务隔离级别
	dbtx, err := dbconn.Begin()
	if err != nil {
		cmd.LogError("Failed to begin transaction: %s", err.Error())
		os.Exit(1)
	}
	defer func() {
		if err != nil {
			_ = dbtx.Rollback()
		}
	}()

	// 初始化事务信息
	_, err = dbtx.Exec(fmt.Sprintf(`
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	SET lock_timeout = 0;
	SET idle_in_transaction_session_timeout = 0;
	SET TRANSACTION SNAPSHOT '%s';
	`, DbSnapShot))
	if err != nil {
		cmd.LogError("Failed to init transaction: %s", err.Error())
		os.Exit(1)
	}

	// // 获取所有的schema的oid
	rows, err := dbtx.Query(`
	select oid 
	from pg_catalog.pg_namespace 
	where nspname NOT LIKE 'pg_temp_%' 
	AND nspname NOT LIKE 'pg_toast%' 
	AND nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog');
	`)
	if err != nil {
		cmd.LogError("Failed get schema oid: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	var schemelist []int
	for rows.Next() {
		var schemaoid int
		err = rows.Scan(&schemaoid)
		if err != nil {
			cmd.LogError("Failed get schema oid: %s", err.Error())
			os.Exit(1)
		}
		schemelist = append(schemelist, schemaoid)
	}

	// 打开源数据文件
	metadatafilepath := fmt.Sprintf("/tmp/gpdbbr_%s_all_metadata.sql", Timestamp)
	metadatafile, err := os.OpenFile(metadatafilepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		cmd.LogError("Failed to open metadata file: %s", err.Error())
		os.Exit(1)
	}
	defer metadatafile.Close()

	for _, schemaoid := range schemelist {
		rows, err = dbtx.Query(fmt.Sprintf("select pg_get_functiondef(oid) from pg_catalog.pg_proc where pronamespace=%d::oid", schemaoid))
		if err != nil {
			cmd.LogError("Failed to get schema function definition: %s", err.Error())
			os.Exit(1)
		}
		defer rows.Close()

		for rows.Next() {
			var funcsql string
			err = rows.Scan(&funcsql)
			if err != nil {
				cmd.LogError("Failed to get schema function definition: %s", err.Error())
				os.Exit(1)
			}

			funcsql = funcsql + ";\n\n"
			_, err = metadatafile.WriteString(funcsql)
			if err != nil {
				cmd.LogError("Failed to get schema function definition: %s", err.Error)
				os.Exit(1)
			}
		}
	}

	// 上传文件到s3
	cmd.PutFileToS3(fmt.Sprintf("/tmp/gpdbbr_%s_all_metadata.sql", Timestamp), fmt.Sprintf("%s/backups/%s/%s/gpdbbr_%s_all_metadata.sql", cmd.ArgConfig.S3Folder, BackupDate, Timestamp, Timestamp))
}

func dumptabddl(tablist []string) {
	dumparg := []string{"-s", fmt.Sprintf("--snapshot=%s", DbSnapShot), cmd.ArgConfig.DbName, "-f", fmt.Sprintf("/tmp/gpdbbr_%s_incr_metadata.sql", Timestamp)}
	for _, tabname := range tablist {
		dumparg = append(dumparg, "-t", tabname)
	}
	ok, output := cmd.ExecOsCmd("pg_dump", dumparg)
	if !ok {
		cmd.LogError("Failed to dump increment table ddl: %s", output)
		os.Exit(1)
	}

	cmd.PutFileToS3(fmt.Sprintf("/tmp/gpdbbr_%s_incr_metadata.sql", Timestamp), fmt.Sprintf("%s/backups/%s/%s/gpdbbr_%s_incr_metadata.sql", cmd.ArgConfig.S3Folder, BackupDate, Timestamp, Timestamp))
}

func bkaotabl(dbconn *sql.DB, tabinfo map[string]interface{}) (int, string, bool, error) {
	dbtx, err := dbconn.Begin()
	if err != nil {
		return 0, "", false, err
	}
	defer func() {
		if err != nil {
			dbtx.Rollback()
		} else {
			dbtx.Commit()
		}
	}()

	_, err = dbtx.Exec(fmt.Sprintf(`
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	SET TRANSACTION SNAPSHOT '%s';
	`, DbSnapShot))
	if err != nil {
		return 0, "", false, err
	}

	colnameagg := fmt.Sprintf("%v", tabinfo["colnameagg"])
	tablename := fmt.Sprintf("%v", tabinfo["tablename"])
	tableoid := fmt.Sprintf("%v", tabinfo["oid"])
	lastddltime := fmt.Sprintf("%v", tabinfo["lastddltimestamp"])
	var lastsegddl string
	if value, exists := tabinfo["ddl_query"]; exists {
		lastsegddl = fmt.Sprintf("%v", value)
	} else {
		lastsegddl = ""
	}

	// 获取AO表的modcount
	var modcount int
	err = dbtx.QueryRow(fmt.Sprintf("SELECT COALESCE(pg_catalog.sum(modcount), 0) AS modcount FROM gp_dist_random('%v')", tabinfo["aosegtablefqn"])).Scan(&modcount)
	if err != nil {
		return 0, "", false, fmt.Errorf("Failed to get AO table modcount: %s", err.Error())
	}

	var isbackupable bool
	if BackupType == "full" {
		isbackupable = true
	} else {
		if modcount != IncrYaml.IncrementalMetadata.AO[tablename].ModCount {
			isbackupable = true
		} else {
			if lastddltime == IncrYaml.IncrementalMetadata.AO[tablename].LastDDLTime {
				isbackupable = false
				return modcount, "", false, nil
			} else {
				if lastsegddl != "" {
					isbackupable = false
					return modcount, lastsegddl, false, nil
				} else {
					isbackupable = true
				}
			}
		}
	}

	if isbackupable {
		copysql := fmt.Sprintf(`
		COPY %s(%s) TO PROGRAM 'gzip -c -1 | %s/bin/gpbackup_s3_plugin backup_data /tmp/gpdbbr_%s_s3.yaml <SEG_DATA_DIR>/backups/%s/%s/gpdbbr_<SEGID>_%s_%s.gz' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;
		`, tablename, colnameagg, GpHome, Timestamp, BackupDate, Timestamp, Timestamp, tableoid)
		timestart := time.Now()
		_, err = dbtx.Exec(copysql)
		if err != nil {
			return 0, "", false, fmt.Errorf("Failed to execute AO table backup sql: %s", err.Error())
		}
		duration := time.Since(timestart).Seconds()
		cmd.LogInfo("Backup AO table done: %s, duration: %.2fs", tablename, duration)
	}

	return modcount, "", true, nil
}

func bkheaptable(dbconn *sql.DB, tabinfo map[string]interface{}) (int, bool, error) {
	dbtx, err := dbconn.Begin()
	if err != nil {
		return 0, false, err
	}
	defer func() {
		if err != nil {
			dbtx.Rollback()
		} else {
			dbtx.Commit()
		}
	}()

	_, err = dbtx.Exec(fmt.Sprintf(`
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
	SET TRANSACTION SNAPSHOT '%s';
	`, DbSnapShot))
	if err != nil {
		return 0, false, err
	}

	colnameagg := fmt.Sprintf("%v", tabinfo["colnameagg"])
	tablename := fmt.Sprintf("%v", tabinfo["tablename"])
	tableoid := fmt.Sprintf("%v", tabinfo["oid"])

	// 获取heap表的maxstat
	// 1.获取表和toast的relfilenode
	getrelfilesql := fmt.Sprintf(`
	select t1.reltablespace, t1.relfilenode, t2.hostname, t2.datadir 
	from (
	select gp_segment_id, reltablespace, relfilenode 
	from pg_class 
	where oid = %s::oid
	union 
    select gp_segment_id, reltablespace, relfilenode 
	from pg_class 
	where oid = (
	select reltoastrelid 
	from pg_class 
	where oid = %s::oid)
	union 
	select gp_segment_id, reltablespace, relfilenode 
	from gp_dist_random('pg_class') 
	where oid = %s::oid
	union  
	select gp_segment_id, reltablespace, relfilenode 
	from gp_dist_random('pg_class') 
	where oid = (
	select reltoastrelid 
	from pg_class 
	where oid = %s::oid)) t1 
	join gp_segment_configuration t2 on t1.gp_segment_id = t2.content 
	and t2.role = 'p'
	`, tableoid, tableoid, tableoid, tableoid)

	rows, err := dbtx.Query(getrelfilesql)
	if err != nil {
		return 0, false, fmt.Errorf("Failed to get heap table file infomation: %s", err.Error())
	}
	defer rows.Close()

	maxstat := 0
	var sshcmd []map[string]string
	for rows.Next() {
		var tbsid, fileid, host, datadir string
		err = rows.Scan(&tbsid, &fileid, &host, &datadir)
		if err != nil {
			return 0, false, fmt.Errorf("Failed to get heap table file infomation: %s", err.Error())
		}

		var cmdstr string
		if tbsid != "0" {
			cmdstr = fmt.Sprintf("stat -c %%Y %s/pg_tblspc/%s/GPDB_7_%s/%d/%s* | sort -n | tail -1", datadir, tbsid, Catavers, DbOid, fileid)
		} else {
			cmdstr = fmt.Sprintf("stat -c %%Y %s/base/%d/%s* | sort -n | tail -1", datadir, DbOid, fileid)
		}
		sshcmd = append(sshcmd, map[string]string{"host": host, "cmd": cmdstr})
	}

	for _, cmds := range sshcmd {
		output, err := cmd.SessPool.ExecuteCommand(cmds["host"], cmds["cmd"])
		if err != nil {
			return 0, false, fmt.Errorf("Failed to get heap table file infomation: %s", err.Error())
		}

		stat, err := strconv.Atoi(output)
		if err != nil {
			return 0, false, fmt.Errorf("Failed to get heap table file infomation: %s", err.Error())
		}

		if stat > maxstat {
			maxstat = stat
		}
	}

	var isbackupable bool
	if BackupType == "full" {
		isbackupable = true
	} else {
		if maxstat > IncrYaml.IncrementalMetadata.Heap[tablename].MaxStat {
			isbackupable = true
		} else {
			var ddlcnt int
			getddlcnt := fmt.Sprintf("select count(*) from logddl.ddl_log where object_name = '%s' and timestamp < to_timestamp('%s', 'YYYYMMDDHH24MISSMS')", tablename, Timestamp)
			err := dbtx.QueryRow(getddlcnt).Scan(&ddlcnt)
			if err != nil {
				return 0, false, fmt.Errorf("Failed to get heap table ddl infomation: %s", err.Error())
			}
			if ddlcnt > 0 {
				// 如果表DDL发生变化，则备份
				isbackupable = true
			} else {
				isbackupable = false
				return maxstat, false, nil
			}
		}
	}

	if isbackupable {
		copysql := fmt.Sprintf(`
		COPY %s(%s) TO PROGRAM 'gzip -c -1 | %s/bin/gpbackup_s3_plugin backup_data /tmp/gpdbbr_%s_s3.yaml <SEG_DATA_DIR>/backups/%s/%s/gpdbbr_<SEGID>_%s_%s.gz' WITH CSV DELIMITER ',' ON SEGMENT IGNORE EXTERNAL PARTITIONS;
		`, tablename, colnameagg, GpHome, Timestamp, BackupDate, Timestamp, Timestamp, tableoid)
		timestart := time.Now()
		_, err = dbtx.Exec(copysql)
		if err != nil {
			return 0, false, fmt.Errorf("Failed to execute heap table backup sql: %s", err.Error())
		}
		duration := time.Since(timestart).Seconds()
		cmd.LogInfo("Backup heap table done: %s, duration: %.2fs", tablename, duration)
	}
	return maxstat, true, nil
}
