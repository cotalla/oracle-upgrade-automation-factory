pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
  }

  environment {
    AWS_REGION = 'us-east-1'
  }

  stages {
    stage('Checkout') {
      steps { checkout scm }
    }

    stage('Validate Repo') {
      steps {
        script {
          if (isUnix()) {
            sh '''
              set -e
              test -f aws-deploy/template.yaml
              test -f aws-deploy/samconfig.toml
              echo "Repo structure OK"
            '''
          } else {
            bat '''
              if not exist aws-deploy\\template.yaml exit /b 1
              if not exist aws-deploy\\samconfig.toml exit /b 1
              echo Repo structure OK
            '''
          }
        }
      }
    }

    stage('SAM Build') {
      steps {
        dir('aws-deploy') {
          script {
            if (isUnix()) {
              sh '''
                set -e
                sam --version
                sam build
              '''
            } else {
              bat '''
                sam --version
                sam build
              '''
            }
          }
        }
      }
    }

    stage('SAM Deploy') {
      steps {
        dir('aws-deploy') {
          script {
            if (isUnix()) {
              sh '''
                set -e
                sam deploy --no-confirm-changeset --no-fail-on-empty-changeset
              '''
            } else {
              bat '''
                sam deploy --no-confirm-changeset --no-fail-on-empty-changeset
              '''
            }
          }
        }
      }
    }
  }

  post {
    success { echo "Deploy succeeded." }
    failure { echo "Deploy failed. Check logs." }
  }
}
