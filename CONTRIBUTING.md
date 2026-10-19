# 参与贡献

## 书稿与图示

提交勘误时注明章、节和原文。涉及协议正确性时，请给出系统假设、事件顺序、持久化状态和可观察结果；涉及引用时，尽量附上一手来源。正文在 `book/chapters/`，目录结构在 `book/data/structure.tex`，机制图源随书稿保存。

修改 TeX 后按[构建说明](docs/building.md)编译，检查引用和溢出警告。书稿修改保持推导和故障前提的完整程度。

## 代码

使用 Python 3.11+；Go 子模块要求 Go 1.24+。在仓库根执行：

```bash
python -m pip install -r requirements-dev.txt
python -m ruff format examples
python -m ruff check examples
cd examples/multipartition/authority
gofmt -w .
go test ./...
```

Windows 可使用 `scripts/check.ps1` 运行格式检查与 Go 测试。行为变化应复用相关故障实验及独立检查器，并在 PR 中说明运行入口、依赖版本和结果。运行目录使用 `examples/<group>/runs/<new-name>`，避免提交数据库、二进制、缓存或安装包。

## 许可

提交书稿或原创图示即同意按 CC BY-SA 4.0 提供该贡献；提交代码即同意按 MIT 提供该贡献。新增第三方内容时一并标明来源和适用许可。
