# 构建整书

## 环境

使用 XeLaTeX 与支持中文的 TeX Live 或 MiKTeX。工程依赖 `ctexbook`、`xeCJK`、`fontspec`、TikZ、`fvextra`、`tabularx`、`booktabs`、`placeins` 等包；完整 TeX 发行版通常已包含这些包。

Ubuntu / Debian 可安装：

```bash
sudo apt-get update
sudo apt-get install texlive-xetex texlive-lang-chinese texlive-latex-extra texlive-fonts-recommended fonts-texgyre
```

macOS 可安装 MacTeX。Windows 使用 TeX Live 或 MiKTeX，并确认 `xelatex --version` 可用。MiKTeX 缺包时先完成包安装，再重新构建。

## 命令

仓库根执行 `.\build.ps1`；Linux / macOS 执行 `bash book/build.sh`。脚本至少运行两遍，最多四遍，直到交叉引用稳定。输出在 `book/build/main.pdf`，诊断在同目录 `main.log`。

字体依次选择系统宋体/微软雅黑、Noto CJK、TeX 自带 Fandol。Fandol 后备字体通过 `.otf` 文件名从 TeX 字体目录加载。西文选择 TeX Gyre 或 Latin Modern；等宽字体优先 Consolas，后备 Latin Modern Mono。中文引语以倾斜字形排版。使用作者原字体时保留原排版，其他字体可能改变分页。

## 检查与清理

检查日志中的缺字、未定义引用、重复目标和 Overfull 信息。修改目录或标签后应重新运行完整构建。

Windows 在仓库根运行：

```powershell
.\scripts\clean.ps1 -WhatIf
.\scripts\clean.ps1
```

清理脚本只处理列明的构建目录、缓存和本地编译二进制。`examples/*/runs/` 中的实验记录由使用者自行归档。

整书 PDF 适合通过 GitHub Releases 发布；Git 仓库保留可复现源文件。CI 会构建 PDF 并保存为工作流产物。
