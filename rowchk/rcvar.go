package rowchk

import "gpdbbr/cmd"

type TabCheckS struct {
	OnlyBk  []string   `yaml:"onlybk"`
	OnlyDb  []string   `yaml:"onlydb"`
	DiffRow []DiffRowS `yaml:"diffrow"`
}

var (
	CnDir  string         // CN目录
	BkMeta cmd.BkMetaData // 元数据
	RptDir string         // 报告目录
	TabCk  TabCheckS      // 表检查配置
	CkTime int            // 检查时间
)

type DiffRowS struct {
	TabName string
	BkRow   float64
	DbRow   float64
}
