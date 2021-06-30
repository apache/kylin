---
layout: dev40
title:  Kylin Document Writing Specification
categories: development
permalink: /development40/doc_spec.html
---

This article describes the Kylin document writing specifications. We will introduce the chapter structure, element tag, term specification, file/path specification, etc.

### Preparation

- Please prepare the environment related to writing documents according to [How to Write Document](/development40/howto_docs.html) and understand the Kylin document structure.

- Kylin documents are written in Markdown syntax, hereinafter referred to as md. Please make sure you are familiar with [Markdown Syntax](https://guides.github.com/features/mastering-markdown/).

### Chapter Structure

- The content of each chapter is organized in multiple subsections, and the heading of each subsection uses **Heading 3 style**. For example:
  \#\#\# Install Kylin
- If you need to further organize the content within the subsections, please use **Unordered / Ordered List**, try not to use **Heading 4**, and avoid **Heading 5** completely. For example:
  \### Install Kylin
  1. First, ...
     \* Run...
     \* Unzip...

### Element Tag

- Bold
  Use bold to mark the content you need to emphasize. Such as:

  - Emphasize the name of a component on the GUI.
  - Emphasize a new concept.
  - Emphasize negative words that users tend to ignore when reading.

- Italic

  - Italics are generally not used in Chinese documents.

  - Italics can be used in English documents for the following situations, such as database name, table name, column name, etc.

- Quote

  - Use quote to mark secondary information / supplementary information, that is, extended information that does not affect normal understanding and use. Such as:

    \> You can continue reading to get more information about...

  - Use quote to mark reminders.
    - For general prompt information, use **Note** at the beginning.
    - For critical or warning messages, start with **Caution**.

- Inline code
  Use inline code to mark everything that **may** be **entered by the user into the shell / config**, such as file paths, Unix accounts, configuration items, and values.

- Code snippet
  Use code snippets to mark **shell commands and config configurations that all users need to execute**, in a unified format and need to be sufficiently prominent. Such as:

  - shell commands

    \`\`\`sh
    $KYLIN_HOME/bin/kylin.sh start
    \`\`\`

  - config configurations 

    \`\`\`properties
    kylin.env.hdfs-working-dir=/kylin
    \`\`\`
    \`\`\` xml
    &lt;property&gt;
    &lt;name&gt;mapreduce.map.memory.mb&lt;/name&gt;
    &lt;value>2048&lt;/value&gt;
    &lt;/property&gt;
    \`\`\`

### Term Specification

- English vocabulary
  - In Chinese documents, the first letter of the English words must be capitalized.  For example:
    Cube 概念是指一个 Cuboid 的集合，其中……。
  - In English documents, when an English-specific vocabulary appears for the first time, the first letter must be capitalized and emphasized in bold. Other times, words such as "cube" or "model" are not required to be capitalized.

- Punctuation
  - In Chinese documents, **Please always use Chinese punctuation**.

- Description of UI interaction
  - Unify the title of page elements.
    - the top header
    - the left navigation
    - the xxx page
    - the xxx panel
    - the xxx dialog

- Use **bold style** to emphasize interactive elements. For example:
  Click the \*\*Submit\*\* button.

- Use **->** to illustrate continuous operation.