# AI Accounting and Financial Analysis

本项目是一个基于 AI 的智能记账与财务分析系统，旨在帮助用户高效、智能地管理个人或小团队的账务数据。

## 功能特性

- 智能语义解析：支持自然语言输入账单，自动结构化解析。
- 多维度账务管理：支持分组、分类、AA 分账、现金流与净资产等多种会计基础。
- 操作日志与撤销：所有操作均有日志记录，支持撤销。
- 数据安全：API 密钥通过环境变量管理，无敏感信息泄露。

## 目录结构

```
project/
    ai_parser.py   # AI 语义解析模块
    config.py      # 配置与密钥管理
    db.py          # 数据库操作
    main.py        # 主程序入口
    service.py     # 业务逻辑
```

## 快速开始

1. 克隆仓库：
   ```bash
   git clone https://github.com/michaelwillia/AI-Accounting-and-Financial-Analysis.git
   ```
2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
3. 配置环境变量：
   - 设置 `DASHSCOPE_API_KEY` 为你的 DashScope API 密钥。
4. 运行主程序：
   ```bash
   python project/main.py
   ```

## 贡献指南

欢迎 issue、PR 及建议！请确保不提交任何敏感信息。

## License

MIT
