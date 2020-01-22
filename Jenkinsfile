#!groovy

pipeline {
    agent none

    stages {
        stage('container') {
            agent {
                dockerfile {
                    args '-v ${HOME}/bin:${HOME}/bin'
                    additionalBuildArgs '--build-arg BUILDER_UID=$(id -u)'
                }
            }
            stages {
                stage('clean') {
                    steps {
                        sh 'git reset --hard'
                        sh 'git clean -xffd'
                    }
                }
                stage('set_version') {
                    when { not { branch "test-release" } }
                    steps {
                        sh './bumpversion.sh build'
                    }
                }
                stage('release') {
                    when { branch 'test-release' }
                    steps {
                        withCredentials([usernamePassword(credentialsId: env.GIT_CREDENTIALS_ID, passwordVariable: 'GIT_PASSWORD', usernameVariable: 'GIT_USERNAME')]) {
                            sh './bumpversion.sh release'
                        }
                    }
                }
                stage('test') {
                    steps {
                        sh 'python3 -m pip install --user -r test_requirements.txt'
                        sh 'pytest'
                    }
                }
                stage('package') {
                    steps {
                        sh 'python3 setup.py bdist_wheel --universal'
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
