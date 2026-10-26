// CI/CD RAG Data Pipeline: test chunk/lineage → kiểm DAG import → build image Airflow
// (feast-venv có feast[postgres] + fastembed) → khởi động lại Airflow → chạy DAG ml_rag_ingest và chờ kết quả.
// Credential lấy từ Vault bằng token chỉ-đọc `vault-ci-token`.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds() }
  environment {
    VAULT_ADDR = 'http://localhost:8200'
    REGISTRY = 'localhost:5001'
    TAG = "${env.GIT_COMMIT ?: env.BUILD_NUMBER}"
  }
  stages {
    stage('Test') {
      steps { sh 'uv run --python 3.12 pytest rag/tests -q' }
    }
    stage('Build') {
      steps { sh 'docker compose --profile airflow build airflow' }
    }
    stage('Push') {
      steps { sh 'scripts/run/push_images.sh platform-airflow:2.10.5-spark3.5.3' }
    }
    stage('Release config') {
      steps {
        sh 'mkdir -p out'
        sh 'scripts/run/compose_release_images.sh airflow=platform-airflow > out/ci-compose-images.yaml'
      }
    }
    stage('Deploy') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN')]) {
          // Compose cần file .env (env_file): dựng tạm từ .env.example + credential Vault, xoá ngay sau đó.
          sh '''
            { cat .env.example
              scripts/setup/vault_env.sh minio | sed 's/^export //'
              scripts/setup/vault_env.sh postgres | sed 's/^export //'; } > .env
            docker compose -f docker-compose.yml -f out/ci-compose-images.yaml --profile airflow up --no-build --pull always -d --wait airflow
            bash scripts/setup/setup_airflow.sh
            rm -f .env
          '''
        }
      }
    }
    stage('DAG import') {
      steps {
        sh '''docker compose -f docker-compose.yml -f out/ci-compose-images.yaml --profile airflow exec -T airflow python -c "
from airflow.models import DagBag
b = DagBag('/opt/airflow/dags', include_examples=False)
assert 'ml_rag_ingest' in b.dags and not b.import_errors, b.import_errors"'''
      }
    }
    stage('Run DAG') {
      steps {
        sh '''
          run=ci_$BUILD_NUMBER
          docker compose -f docker-compose.yml -f out/ci-compose-images.yaml --profile airflow exec -T airflow airflow dags trigger ml_rag_ingest -r $run
          for i in $(seq 1 90); do
            state=$(docker compose -f docker-compose.yml -f out/ci-compose-images.yaml --profile airflow exec -T airflow airflow dags list-runs -d ml_rag_ingest -o plain \
              | awk -v r="$run" '$2 == r {print $3}')
            echo "state=$state"; [ "$state" = success ] && exit 0; [ "$state" = failed ] && exit 1; sleep 10
          done; exit 1
        '''
      }
    }
  }
}
