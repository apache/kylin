node ('nsn_builder_budapest') {
    // This displays colors using the 'xterm' ansi color map.
  ansiColor('xterm') {
        stage("Checkout code") {
            checkout scm
        }
        stage("Set Java to 1.8"){
            sh"export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/; \
            export MVN_HOME=/opt/maven/bin; \
            export PATH=$JAVA_HOME/bin:$MVN_HOME:$PATH;"
        }
        stage("Build Kylin Binaries and Export to S3 bucket"){
            sh"build/script/package.sh; \
            aws s3 cp --recursive dist ${S3_PATH};"
        }
  }
}
