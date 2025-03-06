package restore

import (
	"database/sql"
	"fmt"
	"gpdbbr/cmd"
	"math"
	"time"
)

func findMinGreaterThan(initnum int, nums []int) int {
	min := math.MaxInt64
	for _, num := range nums {
		if num > initnum && num < min {
			min = num
		}
	}

	if min == math.MaxInt64 {
		return -1
	}

	return min
}

func restoredata(dbconn *sql.DB, tabname string, attributest string, taboid string) bool {
	copysql := fmt.Sprintf(`
	COPY %s(%s) FROM PROGRAM '%s/bin/gpbackup_s3_plugin restore_data /tmp/gpdbbr_%s_s3.yaml <SEG_DATA_DIR>/backups/%s/%s/gpdbbr_<SEGID>_%s_%s.gz | gzip -d -c' WITH CSV DELIMITER ',' ON SEGMENT;
	`, tabname, attributest, GpHome, RestoreTime, RestoreDate, RestoreTime, RestoreTime, taboid)
	timestart := time.Now()
	_, err := dbconn.Exec(copysql)
	if err != nil {
		cmd.LogInfo("Failed to restore table %s, error: %s", tabname, err.Error())
		return false
	}
	duratime := time.Since(timestart).Seconds()
	cmd.LogInfo("Restore table %s success, duration: %.2f seconds", tabname, duratime)
	return true
}
