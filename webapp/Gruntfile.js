/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

module.exports = function (grunt) {

  // Project configuration.
  grunt.config.init({
    buildEnv: grunt.option('buildEnv') || 'prod',
    buildNumber: grunt.option('buildNumber') || '0',
    pkg: grunt.file.readJSON('package.json'),
    config: grunt.file.readJSON('grunt.json'),
    bump: {
      options: {
        files: '<%= config.bump.options.files %>',
        commit: true,
        commitFiles: '<%= config.bump.options.commitFiles %>',
        commitMessage: 'Release v%VERSION%',
        createTag: false,
        tagName: 'v%VERSION%',
        tagMessage: 'Version %VERSION%',
        push: false
      }
    },
    changelog: {
      options: {
        dest: 'CHANGELOG.md',
        prepend: true
      }
    },
    clean: {
      build: ['dist/', 'tmp/'],
      tmp: ['tmp/']
    },
    concat: {
      app: {
        src: [
          'tmp/js/templates.js',
          'app/js/**/*.js'
        ],
        dest: 'tmp/js/scripts.js'
      },
      components: '<%= config.concat.components %>',
      css: '<%= config.concat.css %>'
    },
    copy: {
      tmp: {
        files: [
          {
            expand: true,
            cwd: 'app/',
            src: ['**'],
            dest: 'tmp/'
          }
        ]
      },
      build: {
        files: [
          {
            expand: true,
            cwd: 'tmp/',
            src: ['index.html', 'js/scripts.min.js', 'image/**', 'css/styles.min.css', 'routes.json'],
            dest: 'dist/',
            rename: function (desc, src) {
              return desc + src.replace('min.', 'min.<%= buildNumber %>.')
            }
          },
          {
            expand: true,
            cwd: 'tmp/',
            src: ['htaccess.dist'],
            dest: 'dist/',
            rename: function (dest, src) {
              console.log(src);
              if (src == 'htaccess.dist') {
                return dest + '.htaccess';
              }
            }
          },
          {
            expand: true,
            cwd: 'app/components/font-awesome/',
            src: ['fonts/*'],
            dest: 'dist/'
          },
          {
            expand: true,
            cwd: 'app/components/bootstrap/dist/',
            src: ['fonts/*'],
            dest: 'dist/'
          },
          {
            expand: true,
            cwd: 'app',
            src: ['fonts/*'],
            dest: 'dist/'
          },
          {
            expand: true,
            cwd: 'app/components/ace/',
            src: ['fonts/*'],
            dest: 'dist/'
          },
          {
            expand: true,
            cwd: 'app/components/angular-tree-control/images/',
            src: ['*'],
            dest: 'dist/images'
          },
          {
            expand: true,
            cwd: 'app/components/chosen/',
            src: ['*.png'],
            dest: 'dist/css'
          },
          {
            expand: true,
            cwd: 'app/components/angular-ui-grid/',
            src: ['ui-grid.eot', 'ui-grid.svg', 'ui-grid.ttf', 'ui-grid.woff'],
            dest: 'dist/css'
          },
          {
            expand: true,
            cwd: 'app/components/ace-builds/',
            src: ['src-min-noconflict/worker-json.js'],
            flatten: true,
            dest: 'dist/'
          }
        ]
      }
    },
    html2js: {
      partials: {
        src: ['app/partials/**/*.html'],
        dest: 'tmp/js/templates.js',
        module: 'templates',
        options: {
          base: 'app/'
        }
      }
    },
    htmlrefs: {
      dist: {
        src: 'tmp/index.html',
        dest: 'tmp/'
      },
      options: {
        buildNumber: '<%= buildNumber %>'
      }
    },
    jshint: {
      files: ['app/js/**/*.js', '!app/components/**'],
      options: {
        jshintrc: '.jshintrc'
      }
    },
    less: {
      prod: {
        options: {
          compress: true,
          yuicompress: true
        },
        files: {
          'tmp/css/styles.css': 'tmp/less/build.less'
        }
      }
    },
    lesslint: {
      src: ['app/less/**.less']
    },
    manifest: {
      build: {
        options: {
          basePath: 'dist/',
          timestamp: true
        },
        src: ['**/**.**'],
        dest: 'dist/manifest.appcache'
      }
    },
    'regex-replace': {
      strict: {
        src: ['tmp/js/scripts.js'],
        actions: [
          {
            name: 'use strict',
            search: '\\\'use strict\\\';',
            replace: '',
            flags: 'gmi'
          }
        ]
      },
      manifest: {
        src: ['tmp/index.html'],
        actions: [
          {
            name: 'manifest',
            search: '<html>',
            replace: '<html manifest="manifest.appcache">'
          }
        ]
      },
      templates: {
        src: ['tmp/js/scripts.js'],
        actions: [
          {
            name: 'templates',
            search: /kylin\', \[/,
            replace: 'kylin\', [\'templates\',',
            flags: 'gmi'
          }
        ]
      }
    },
    uglify: {
      app: {
        options: {
          mangle: false
        },
        files: {
          'tmp/js/scripts.js': 'tmp/js/scripts.js'
        }
      }
    },
    karma: {
      unit: {
        configFile: 'karma.unit.conf.js'
      },
      e2e: {
        configFile: 'karma.e2e.conf.js'
      }
    },
    watch: {
      appchange: {
        options: {
          livereload: true,
        },
        files: [
          "app/css/*.*",
          "app/fonts/*.*",
          "app/image/*.*",
          "app/js/*.*",
          "app/less/*.*",
          "app/partials/**/*.*",
          "app/*.*"
        ],
        tasks: ['dev']
      }
    },
    connect: {
      options: {
        port: 7071,      // custom port
        base: './dist',  // current directory for 'index.html' is root
        livereload: true,
      },
      rules: [           // rewrite rules for static resources
        {from: '^/kylin/routes.json$', to: '/routes.json'},
        {from: '^/kylin/((css|fonts|image|js)/.*)$', to: '/$1'},
        {from: '^/kylin/(?!api/).*$', to: '/'},
      ],
      devserver: {
        options: {
          middleware: function (connect, options) {
            var rewriteRulesSnippet = require('grunt-connect-rewrite/lib/utils').rewriteRequest;
            var proxySnippet = require('grunt-connect-proxy/lib/utils').proxyRequest;
            var middlewares = [rewriteRulesSnippet, proxySnippet];
            if (!Array.isArray(options.base)) {
              options.base = [options.base];
            }
            var directory = options.directory || options.base[options.base.length - 1];
            options.base.forEach(function (base) {
              middlewares.push(connect.static(base));
            });
            middlewares.push(connect.directory(directory));
            return middlewares;
          }
        },
        proxies: [
          {
            context: '/kylin/api',
            host: '127.0.0.1',
            port: '7070'
          }
        ]
      }
    }
  });

  // Additional task plugins
  grunt.loadNpmTasks('grunt-contrib');
  grunt.loadNpmTasks('grunt-conventional-changelog');
  grunt.loadNpmTasks('grunt-bump');
  grunt.loadNpmTasks('grunt-html2js');
  grunt.loadNpmTasks('grunt-htmlrefs');
  grunt.loadNpmTasks('grunt-lesslint');
  grunt.loadNpmTasks('grunt-manifest');
  grunt.loadNpmTasks('grunt-regex-replace');
  grunt.loadNpmTasks('grunt-karma');

  grunt.registerTask('test', ['jshint', 'lesslint']);

  grunt.registerTask('prod', [
//    'test',
    'clean:build',
    'copy:tmp',
    'html2js',
    'concat:app',
    'regex-replace:strict',
    'regex-replace:templates',
    'uglify',
    'regex-replace:manifest',
    'concat:components',
    'less',
    'concat:css',
    'htmlrefs',
    'copy:build',
    'clean:tmp',
    'manifest',
    'changelog'
  ]);

  grunt.registerTask('dev', [
//    'test',
    'clean:build',
    'copy:tmp',
    'html2js',
    'concat:app',
    'regex-replace:strict',
    'regex-replace:templates',
//        'uglify',
    'regex-replace:manifest',
    'concat:components',
    'less',
    'concat:css',
    'htmlrefs',
    'copy:build',
    'clean:tmp',
    'manifest',
    'changelog'
  ]);

  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-connect-proxy');
  grunt.loadNpmTasks('grunt-connect-rewrite');

  grunt.registerTask('devserver', function (target) {
    grunt.task.run([
      'dev',
      'configureRewriteRules',
      'configureProxies:devserver',
      'connect:devserver',
      'watch:appchange',
    ]);
  });
};
