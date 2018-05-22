pipeline { 
    agent any 
    stages {
        stage('Build') { 
            steps { 
            	withMaven(maven : 'Maven') {
                    sh 'mvn clean compile' 
            	}
            }
        }
        stage('Test'){
            steps {
                withMaven(maven : 'Maven') {
                	sh 'mvn test'
                } 
            }
        }
        stage('Deploy') {
            steps {
            	withMaven(maven : 'Maven') {
                    sh 'mvn deploy'
                }
            }
        }
    }
}
