pipeline {
    agent any
    stages {
        stage('rocketmq-cluster start'){
            steps {
                sh 'docker run -d --name rmqnamesrv rocketmqinc/rocketmq:4.5.0 sh mqnamesrv'
                sh 'docker run -d --name rmqbroker --link rmqnamesrv:namesrv -e "NAMESRV_ADDR=namesrv:9876" rocketmqinc/rocketmq:4.5.0  sh mqbroker'
                sh 'sleep 10'
            }
        }
        stage('CentOS 6'){
            agent {
                dockerfile {
                    filename 'Dockerfile.centos6'
                    args '-u root -e "NAMESRV_ADDR=namesrv:9876" --link rmqnamesrv:namesrv'
                }
            }
            steps {
                sh 'go test -v ./core ./test'
            }
        }
        stage('CentOS 7'){
            agent {
                dockerfile {
                    filename 'Dockerfile.centos7'
                    args '-u root -e "NAMESRV_ADDR=namesrv:9876" --link rmqnamesrv:namesrv'
                }
            }
            steps {
                sh 'go test -v ./core ./test'
            }
        }
    }
    post {
        always {
            sh 'docker stop  `docker ps -aq --filter name=rmqbroker`'
            sh 'docker rm  `docker ps -aq --filter name=rmqbroker`'
            sh 'docker stop  `docker ps -aq --filter name=rmqnamesrv`'
            sh 'docker rm  `docker ps -aq --filter name=rmqnamesrv`'
        }
    }
}