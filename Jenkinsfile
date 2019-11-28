switch(env.ENV) {
  case "dev":
    exec_node = 'nsn_deployer'
    break
  case "qa":
    exec_node = 'nsn_deployer'
    break
  case "cicd":
    exec_node = 'nsn_deployer'
    break
  case "pentest.pentest":
    exec_node = 'pentest_deployer'
    break
  case "pentest2.pentest":
    exec_node = 'pentest_eployer'
    break
  default:
    exec_node = 'nsn_deployer'
    break
}
node (exec_node) {
    // This displays colors using the 'xterm' ansi color map.
  ansiColor('xterm') {
        stage("Checkout code") {
            checkout scm
        }
        stage("Set Java to 1.8"){
            sh"export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/; \
            export PATH=$JAVA_HOME/bin:$PATH;"
        }
        stage("Build Kylin Binaries"){
            sh"cd kylin && build/script/package.sh; \
            aws s3 cp --recursive dist ${S3_PATH};"
        }
  }
}
