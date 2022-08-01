const fs = require('fs');
const path = require('path');
const npmPath = process.env.PWD;

const configs = parseArgConfig([
  '--rootPath',
  '--configFile',
]);

let cacheResult = null;
/*
{ "filePath": {
  [rowIdx]: {
    colIdx: 1,
    content: 'aaa 我是中文'
  }
} }
*/
const chineseMap = {};

travellingFiles(resolvePath(configs.rootPath), (filePath) => {
  process.stdout.write(`Scan chinese in ${filePath}... `);
  if (!checkExcludeFile(filePath)) {
    const fileContent = removeFileComments(filePath);
    chineseMap[filePath] = findContentChinese(fileContent);
  }
  process.stdout.write(`Done.\n`);
});

const hasChinese = Object.values(chineseMap).some(chineseResults => chineseResults.length);

if (hasChinese) {
  for (const [filePath, chineseLines = []] of Object.entries(chineseMap)) {
    const isChineseFile = !!chineseLines.length;
    if (isChineseFile) {
      for (const chineseLine of chineseLines) {
        outputChineseLine({ ...chineseLine, filePath });
      }
    }
  }
  throw new Error('Frontend code has chinese! Please check logs.');
}


// Functions
function parseArgConfig(argumentKeys = []) {
  let config = {
    rootPath: './src',
  };
  // 读取arguments里面的配置
  for (const key of argumentKeys) {
    if (typeof config[key.replace('--', '')] === 'boolean') {
      const keyIndex = process.argv.findIndex(arg => arg === key);
      config = { ...config, [key.replace('--', '')]: keyIndex !== -1 };
    } else {
      const keyIndex = process.argv.findIndex(arg => arg === key);
      const value = process.argv[keyIndex + 1];
      if (value && keyIndex !== -1) {
        config = { ...config, [key.replace('--', '')]: value };
      }
    }
  }
  // 如果arguments里面有配置文件，则用文件的配置覆盖arguments里面的配置
  if (config.configFile) {
    const fileConfig = readConfigFile(config.configFile);
    config = { ...config, ...fileConfig };
  }
  return config;
}

function resolvePath(filePath) {
  return path.resolve(npmPath, filePath);
}

function readConfigFile(filePath) {
  return require(resolvePath(filePath))
}

function travellingFiles(filePath, callback) {
  const stat = fs.statSync(filePath);

  if (stat.isDirectory()) {
    const children = fs.readdirSync(filePath);
    for (const child of children) {
      travellingFiles(path.resolve(filePath, child), callback);
    }
  } else if (stat.isFile()) {
    callback(filePath);
  }
}

function removeFileComments(filePath) {
  switch (path.extname(filePath)) {
    case '.vue':
      return removeVueComments(filePath);
    case '.js':
    case '.css':
    case '.less':
      return removeJSComments(filePath);
    default:
      return undefined;
  }
}

function removeVueComments(filePath) {
  let content = fs.readFileSync(filePath, 'utf-8');
  content = content.replace(/<!--\s*[\w\W\r\n]*?\s*-->/gmi, '');
  content = removeHTMLComments(content, /<template\b[^>]*>([^]*)<\/template>/im);
  content = removeScriptComments(content, /<script\b[^>]*>([^]*)<\/script>/im);
  content = removeScriptComments(content, /<style\b[^>]*>([^]*)<\/style>/im);
  return content;
}

function removeJSComments(filePath) {
  let content = fs.readFileSync(filePath, 'utf-8');
  content = removeScriptComments(content);
  return content;
}

function removeHTMLComments(content = '', regex) {
  let cleanedContent = content;
  if (regex) {
    // 获取HTML内容
    const htmlContent = content.match(regex) && content.match(regex)[0];

    if (htmlContent) {
      const cleanedHTMLContent = htmlContent
        // 去掉 <!--注释-->
        .replace(/<!--\s*[\w\W\r\n]*?\s*-->/gmi, '');
      cleanedContent = content.replace(htmlContent, cleanedHTMLContent);
    }
  } else {
    const cleanedHTMLContent = content
      // 去掉 <!--注释-->
      .replace(/<!--\s*[\w\W\r\n]*?\s*-->/gmi, '');
    cleanedContent = content.replace(content, cleanedHTMLContent);
  }
  return cleanedContent;
}

function removeScriptComments(content = '', regex) {
  let cleanedContent = content;
  if (regex) {
    // 获取script内容
    const scriptContent = content.match(regex) && content.match(regex)[0];

    if (scriptContent) {
      const cleanedScriptContent = removeLocales(scriptContent)
        // 去掉 /* 注释 */
        .replace(/\/\*[\w\W\r\n]*?\*\//gmi, '')
        // 去掉 // 注释 TODO: 字符串里面带双斜杠会被干掉，这个有待研究
        .replace(/\/\/[\w\W]*?\n/gmi, '\n');
      cleanedContent = content.replace(scriptContent, cleanedScriptContent);
    }
  } else {
    const cleanedScriptContent = removeLocales(content)
      // 去掉 /* 注释 */
      .replace(/\/\*[\w\W\r\n]*?\*\//gmi, '')
      // 去掉 // 注释 TODO: 字符串里面带双斜杠会被干掉，这个有待研究
      .replace(/\/\/[\w\W]*?\n/gmi, '\n');
    cleanedContent = content.replace(content, cleanedScriptContent);
  }
  return cleanedContent;
}

function findContentChinese(fileContent = '') {
  const results = [];
  const textLines = fileContent.split('\n');
  textLines.forEach((text, rowIdx) => {
    const result = text.match(/[\u4e00-\u9fa5]/);
    if (result) {
      const { index: colIdx } = result;
      results.push({ rowIdx, colIdx, text });
    }
  });
  return results;
}

function cutToLastChar(string = '', char = '') {
  const lastIndex = string.lastIndexOf(char) + 1;
  return {
    text: lastIndex !== -1 ? string.slice(0, lastIndex) : '',
    lastIndex
  };
}

function removeLocales(content = '') {
  let resultString = content;
  const localesResult = content.match(/locales:\s*{/);
  if (localesResult) {
    const startIndex = localesResult.index;
    const lastIndex = startIndex +
      + localesResult[0].length
      + findLocalesLastIndex(`{${content.split(/locales:\s*{/)[1]}`);
    resultString = content.replace(content.slice(startIndex, lastIndex), '');
  }
  return resultString;
}

function findLocalesLastIndex(string = '', lastIndex) {
  try {
    eval(`cacheResult = ${string};`);
    return lastIndex;
  } catch {
    const removedResult = cutToLastChar(string.slice(0, string.length - 1), '}');
    return removedResult.text ? findLocalesLastIndex(removedResult.text, removedResult.lastIndex) : null;
  }
}

function checkExcludeFile(filePath = '') {
  return configs.excludes && configs.excludes.some(exclude => exclude.test(filePath));
}

function outputChineseLine({ rowIdx, colIdx, text, filePath }) {
  console.log(`Line ${rowIdx}:${colIdx}:  Find chinese in file ${filePath}`);
  const perfixInfo = `> ${rowIdx} | `;
  console.log(`${perfixInfo}${text}`);
  for (let i = 0; i < perfixInfo.length + colIdx; i += 1) {
    process.stdout.write(' ');
  }
  console.log('^');
  console.log('\r\n');
}
