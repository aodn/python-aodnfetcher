#!groovy

pipeline {
    agent { label 'master' }

    stages {
        stage('clean') {
            steps {
                sh 'git clean -fdx'
            }
        }
        stage('set_version') {
            steps {
                sh 'bumpversion patch'
            }
        }
        stage('release') {
            when { branch 'master' }
            steps {
                sh 'bumpversion --tag --commit --allow-dirty release'
            }
        }
        stage('container') {
            agent {
                dockerfile {
                    additionalBuildArgs '--build-arg BUILDER_UID=${JENKINS_UID:-9999}'
                }
            }
            stages {
                stage('test') {
                    steps {
                        sh 'python setup.py test'
                    }
                }
                stage('package') {
                    steps {
                        sh 'python setup.py bdist_wheel'
                    }
                }
            }
            post {
                success {
                    dir('dist/') {
                        archiveArtifacts artifacts: '*.whl', fingerprint: true, onlyIfSuccessful: true
                    }
                }
            }
        }
    }
}
