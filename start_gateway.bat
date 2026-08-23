@echo off
rem LLM网关静默启动脚本（供计划任务/开机自启调用）
cd /d D:\xuex3\java_web\luyou
D:\xuex3\java_web\luyou\.venv\Scripts\python.exe main.py --config config.yaml
