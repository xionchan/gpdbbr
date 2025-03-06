package restore

import "gpdbbr/cmd"

var (
	RestoreType string         // 恢复类型
	RestoreTime string         // 恢复时间戳
	RestoreDate string         // 恢复日期
	GpHome      string         // GP 主目录
	CnDir       string         // CN 目录
	HostList    []string       // 主机列表
	BkYaml      cmd.BkMetaData // 备份元数据
	RestoreRpt  cmd.RsRpt      // 恢复报告
)
