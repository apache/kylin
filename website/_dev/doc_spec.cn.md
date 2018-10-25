---
layout: dev-cn
title:  Kylin 文档撰写规范
categories: development
permalink: /cn/development/doc_spec.html
---


本文从章节结构、元素标记、用语规范、文件/路径规范等方面对 Kylin 文档的撰写规范进行了详述。

### 准备工作

1. 请您根据 [如何写文档](howto_docs.cn.md) 准备撰写文档有关的环境，了解 Kylin 文档结构。
2. Kylin 文档使用 Markdown 语法书写，以下简称 md。请您确保您熟悉 [Markdown 语法](https://guides.github.com/features/mastering-markdown/)。

### 章节结构

- 每个章节的内容以多个小节的形式组织，每个小节的标题使用 **Heading 3 样式**。如：
	\#\#\# 安装 Kylin
- 如果需要在小节内进一步对内容进行组织，请使用 **无序 / 有序 列表**，尽量不使用 **Heading 4**，完全避免 **Heading 5**。如：
	\### 安装 Kylin
	1. 首先，……
        \* 运行……
        \* 解压……

### 元素标记

- 粗体
  使用粗体标记您需要强调的内容。如：
  1. 强调 GUI 上某个组件的名称。
  2. 强调一个新概念。
  3. 强调用户在阅读时容易忽略的否定词。

- 斜体
  1. 中文文档中一般不使用斜体。
  2. 英文文档中对于以下情形可以使用斜体，如数据库表名、列名等。

- 引用
  1. 使用引用来标记 次要信息 / 补充信息，即不影响正常理解和使用的扩展信息。如：
  	&gt; 您可以继续阅读以获得更多关于……的信息。
  2. 使用引用来标记 提示信息。
  	- 对于一般性提示信息，使用 **提示 / Note** 开头。
  	- 对于关键或警示的提示信息，使用 **注意 / Caution** 开头。

- 行内代码
  使用行内代码标记一切**可能**会**被用户输入到 shell / config 中的内容**，比如文件路径、 Unix 账户、配置项和值等。

- 代码段
  使用代码段标记**所有用户需要执行的 shell 命令和 config 配置**，统一格式且需要足够凸显。如：

  1. shell 命令
  \`\`\`shell
  $KYLIN_HOME/bin/kylin.sh start
  \`\`\`

  2. config 配置
    \`\`\`properties
    kylin.env.hdfs-working-dir=/kylin
    \`\`\`
    \`\`\` xml
    &lt;property&gt;
    &lt;name&gt;mapreduce.map.memory.mb&lt;/name&gt;
    &lt;value>2048&lt;/value&gt;
    &lt;/property&gt;
    \`\`\`


### 用语规范

- 英文专用词汇
  - 中文文档中，一般出现的英文词汇都需要使用首字母大写。如：
  	Cube 概念是指一个 Cuboid 的集合，其中……。
  - 英文文档中，当第一次出现某个英语专有词汇时，需要将首字母大写，并且用粗体强调，其他时候不需要大写 ”cube“ 或者 ”model“ 等词语。

- 中英文（数字）混合
  在中文版中，所有出现的英文（数字）需要在两端中英文交界处添加一个**额外英文半角空格**，以增强中英文混排的美观性和可读性。
- 标点符号
  - 在中文文档中，**请一律使用中文标点符号**。

- UI 交互的描写
  1. 统一对页面元素的称呼。
    顶部状态栏 / the top header
    左侧导航栏 / the left navigation
    xxx 页面 / the xxx page
    xxx 面板 / the xxx panel
    xxx 对话框 / the xxx dialog
  2. 用**加粗样式**强调交互元素。如：
    点击\*\*提交\*\*按钮。
  3. 用 **->** 说明连续操作。