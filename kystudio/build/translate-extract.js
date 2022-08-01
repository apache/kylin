const fs = require('fs');
const path = require('path');
const npmPath = process.env.PWD;

const configs = parseArgConfig([
  '--rootPath',
  '--output',
  '--configFile',
  '--dismissNull'
]);

let cacheResult = null;
const translateMap = {};

travellingFiles(resolvePath(configs.rootPath), (filePath) => {
  process.stdout.write(`Start translating ${filePath}... `);
  // 翻译文件，输出翻译后的object
  const tranlsateResult = translateFile(filePath);
  // 非.vue和.js的文件会输出undefined，所以过滤掉
  if (tranlsateResult !== undefined) {
    if (!(configs.dismissNull && tranlsateResult === null)) {
      translateMap[filePath] = tranlsateResult;
    }
  }
  process.stdout.write(`Done.\n`);
});

if (!configs.bundles) {
  process.stdout.write(`Writing translating file... `);
  fs.writeFileSync(resolvePath(`${configs.output}.json`), JSON.stringify(translateMap, null, 2));
  process.stdout.write(`Done.\n`);
} else {
  process.stdout.write(`Writing translating file... `);
  outputBundleFiles(translateMap, configs.bundles);
  process.stdout.write(`Done.\n`);
}




// Functions
function parseArgConfig(argumentKeys = []) {
  let config = {
    rootPath: './src',
    output: './message',
    configFile: null,
    bundles: null,
    dismissNull: false,
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

function translateFile(filePath) {
  switch (path.extname(filePath)) {
    case '.vue':
      return readVueTranslate(filePath);
    case '.js':
      return readJSTranslate(filePath);
    default:
      return undefined;
  }
}

function readVueTranslate(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const scriptRegex = /<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi;
  const scriptContent = content.match(scriptRegex) && content.match(scriptRegex)[0];

  if (scriptContent && /locales:\s*{/.test(scriptContent)) {
    return findLocalesObj(`{${scriptContent.split(/locales:\s*{/)[1]}`);
  }
  return null;
}

function readJSTranslate(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  if (
    content && (
      /"en"\s*:\s*{/.test(content) ||
      /en\s*:\s*{/.test(content) ||
      /'en'\s*:\s*{/.test(content)
    )
  ) {
    return findLocalesObj(`{${content.split(/export\s*default\s*{/)[1]}`);
  }
  return null;
}

function removeLastChar(string = '', char = '') {
  const lastIndex = string.lastIndexOf(char);
  return lastIndex !== -1 ? string.slice(0, lastIndex - 1) : '';
}

function findLocalesObj(string = '') {
  try {
    eval(`cacheResult = ${string};`);
    return cacheResult;
  } catch {
    const removedString = removeLastChar(string, '}');
    return removedString ? findLocalesObj(removedString) : null;
  }
}

function travellingBundles(currentBundle = {}, parentPath = '') {
  let bundleMap = {};

  for (const folder in currentBundle) {
    const folderRule = currentBundle[folder];

    if (Object.prototype.toString.call(folderRule) === '[object Object]') {
      const subBundleMap = travellingBundles(folderRule, parentPath ? `${parentPath}/${folder}` : folder);
      bundleMap = { ...bundleMap, ...subBundleMap };
    } else {
      bundleMap[parentPath ? `${parentPath}/${folder}` : folder] = folderRule;
    }
  }
  return bundleMap;
}

function parseBundleConfigs(bundleConfigs) {
  return Object
    .entries(travellingBundles(bundleConfigs, configs.output))
    .reduce((bundleMap, [bundlePath, bundleRule]) => ({
      ...bundleMap,
      [resolvePath(bundlePath)]: bundleRule,
    }), {});
}

function validateBundleRule(bundleRule, filePath) {
  if (Object.prototype.toString.call(bundleRule) === '[object Array]') {
    for (const itemRule of bundleRule) {
      if (itemRule.test(filePath)) return true;
    }
    return false;
  } else if (Object.prototype.toString.call(bundleRule) === '[object RegExp]') {
    return bundleRule.test(filePath);
  } else if (bundleRule === '*') {
    return true;
  }
  return false;
}

function outputBundleFiles(translationMap, bundleConfigs) {
  let outputBundleMap = {};
  const bundleMap = parseBundleConfigs(bundleConfigs);
  for (const [translatedFilePath, translateContent] of Object.entries(translationMap)) {
    const [bundlePath] = Object.entries(bundleMap)
      .find(([, rule]) => validateBundleRule(rule, translatedFilePath));

    outputBundleMap = {
      ...outputBundleMap,
      [`${bundlePath}.json`]: {
        ...outputBundleMap[`${bundlePath}.json`],
        [translatedFilePath]: translateContent,
      }
    };
  }

  console.log(outputBundleMap);

  for (const [bundlePath, bundleContent] of Object.entries(outputBundleMap)) {
    const dirpath = path.dirname(bundlePath)
    if (!fs.existsSync(dirpath)) {
      fs.mkdirSync(dirpath);
    }
    fs.writeFileSync(bundlePath, JSON.stringify(bundleContent, null, 2));
  }
}
