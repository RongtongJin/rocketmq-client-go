pipeline {
    agent none
    stages {
        stage('Test') {
            parallel {
                stage('CentOS 6'){
                    agent {
                        dockerfile {
                            filename 'Dockerfile.centos6'
                            args '-u root'
                        }
                    }
                    steps {
                        sh 'go test -v test/producer_test.go test/util.go 2>&1 > tmp'
                        sh '$GOPATH/bin/go-junit-report < tmp > test_output.xml'
                        junit '*.xml'
                    }
                }
                stage('CentOS 7'){
                    agent {
                        dockerfile {
                            filename 'Dockerfile.centos7'
                            args '-u root'
                        }
                    }
                    steps {
                        sh 'go test -v test/producer_test.go test/util.go 2>&1 > tmp'
                        sh '$GOPATH/bin/go-junit-report < tmp > test_output.xml'
                        junit '*.xml'
                    }
                }
            }

        }
    }
}