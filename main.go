package main

import (
	"gpdbbr/backup"
	"gpdbbr/cmd"
	"gpdbbr/restore"
	"gpdbbr/rowchk"
	"log"
	"os"
	"runtime/debug"
)

func main() {
	// 捕获异常
	defer func() {
		if err := recover(); err != nil {
			log.Println("panic:", err)
			debug.PrintStack()
			os.Exit(1)
		}
	}()

	// 解析参数
	cmd.ParseArg()

	// 备份
	if cmd.ArgConfig.Type == "backup" {
		backup.DoBackup()
	} else if cmd.ArgConfig.Type == "restore" {
		// 还原
		restore.DoRestore()
	} else {
		rowchk.DoRowChk()
	}
}
