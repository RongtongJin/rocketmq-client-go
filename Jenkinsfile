pipeline {
    agent any
    stages {
        stage('rocketmq cluster start'){
            steps {
                sh 'docker run -d --name rmqnamesrv apacherocketmq/rocketmq:4.5.0 sh mqnamesrv'
                sh 'docker run -d --name rmqbroker --link rmqnamesrv:namesrv -e "NAMESRV_ADDR=namesrv:9876" -e "JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.232.b09-0.el7_7.x86_64/jre" apacherocketmq/rocketmq:4.5.0  sh mqbroker'
                sh 'sleep 10'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_normal_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_oneway_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_rebalance_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_broadcast_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_tag_filter_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_partition_order_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_transaction_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_too_long_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_delay_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_chinese_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_mutil_topic1_test'
                sh 'docker exec rmqbroker sh ./mqadmin updateTopic -n namesrv:9876 -b localhost:10911 -t go_mutil_topic2_test'
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
                sh 'go test -v ./core ./test -cover -coverpkg ./core| tee tmp'
                sh '$GOPATH/bin/go-junit-report < tmp > test_output.xml'
                junit '*.xml'
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
                sh 'go test -v ./core ./test -cover | tee tmp'
                sh '$GOPATH/bin/go-junit-report < tmp > test_output.xml'
                junit '*.xml'
            }
        }
        stage('Debian'){
            agent {
                dockerfile {
                    filename 'Dockerfile.debian'
                    args '-u root -e "NAMESRV_ADDR=namesrv:9876" --link rmqnamesrv:namesrv'
                }
            }
            steps {
                sh 'go test -v ./core ./test -cover | tee tmp'
                sh '$GOPATH/bin/go-junit-report < tmp > test_output.xml'
                junit '*.xml'
            }
        }
        stage('Ubuntu'){
            agent {
                dockerfile {
                    filename 'Dockerfile.ubuntu'
                    args '-u root -e "NAMESRV_ADDR=namesrv:9876" --link rmqnamesrv:namesrv'
                }
            }
            steps {
                sh 'go test -v ./core ./test -cover | tee tmp'
                sh '$GOPATH/bin/go-junit-report < tmp > test_output.xml'
                junit '*.xml'
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