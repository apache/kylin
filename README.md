# How to write document

### Before your start
Learn more from https://docusaurus.io/ .

#### Install nodejs
Make sure node.js 16.14 or above is installed.

Check version
```shell
node -v
```

```shell
sudo npm cache clean -f
sudo npm install -g n
sudo n stable
sudo npm install npm@latest -g
```

#### Directories
```text
website
├── blog 
│   ├── 2019-05-29-welcome.md
├── docs 
│   ├── doc1.md
│   └── mdx.md
├── src 
│   ├── css 
│   │   └── custom.css
│   └── pages 
│       ├── styles.module.css
│       └── index.js
├── static 
│   └── img
├── docusaurus.config.js 
├── package.json 
├── README.md
├── sidebars.js
└── yarn.lock
```



### Preview at local machine

```shell

## Starts the development server.
npm start

## Publishes the website to GitHub pages.
npm run deploy
```

### Website TODO List

- [x] Search in document
- [ ] SEO
- [ ] Multi Version
- [ ] i18n

### Tech Article
- [ ] MetadataStore and Job Engine
- [ ] New Frontend and New Modeling Process
- [ ] Index Management
