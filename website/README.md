# How to write document

### Before your start
Learn more from https://docusaurus.io/ .

#### Install nodejs
Make sure node.js 18.20.4 or above is installed.

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
├── docusaurus.config.ts
├── package.json 
├── README.md
├── sidebars.ts
└── yarn.lock
```



### Preview at local machine

```shell

## Starts the development server.
cd website
npm start

## Build HTML from source
npm build

## Publishes the website to GitHub pages.
npm deploy
```

### How to versioning docs
We use [Docusaurus](https://docusaurus.io/docs/versioning) versioning feature to manage docs.
To generate a new version snapshot, run the following command:
```sh
npm run docusaurus docs:version 4.0.4
```
This will create a new versioned docs folder under `website/versioned_docs/` and sidebar file under `website/versioned_sidebars`

### How to push to apache website

TODO
