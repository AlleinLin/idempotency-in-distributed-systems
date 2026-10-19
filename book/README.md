# TeX 源工程

主文件是 `main.tex`，使用 XeLaTeX。完整正文、参考文献、章首艺术图片及可编辑 TikZ 图源均在本目录。

```powershell
.\build.ps1
```

```bash
bash build.sh
```

PDF 输出到 `build/main.pdf`。依赖与字体后备见[构建说明](../docs/building.md)，章节导航见[阅读指南](../docs/reading-guide.md)。

`data/structure.tex` 定义章、节及部分标题；`styles/` 定义排版；`frontmatter/` 包含封面、前言和概论；`backmatter/` 包含全书结语与参考文献；`assets/` 保存排版使用的图片。
