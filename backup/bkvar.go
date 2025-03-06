package backup

import "gpdbbr/cmd"

var (
	BackupType string         // 备份类型
	Catavers   string         // catalog 版本号码
	DbSnapShot string         // 数据库快照号
	UnixTime   string         // 备份的unix时间戳
	Timestamp  string         // 备份时间戳
	BackupDate string         // 备份日期
	GpHome     string         // GP 主目录
	HostList   []string       // 主机列表
	DbOid      int            // 数据库 OID
	IncrYaml   cmd.BkMetaData // 增量备份元数据
	BkResult   cmd.BkMetaData // 备份结果
)
