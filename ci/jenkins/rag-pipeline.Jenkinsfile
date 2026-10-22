// CI/CD RAG Data Pipeline (rubric mle dòng 32): test chunk/lineage → kiểm DAG import → build image Airflow
// (feast-venv có feast[postgres] + fastembed) → khởi động lại Airflow → chạy DAG ml_rag_ingest và chờ kết quả.
// Credential lấy từ Vault bằng token chỉ-đọc `vault-ci-token`.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds() }
  environment { VAULT_ADDR = 'http://localhost:8200' }
  stages {
    stage('Test') {
      steps { sh 'uv run --python 3.12 pytest rag/tests -q' }
    }
    stage('Build') {
      steps { sh 'docker compose --profile airflow build airflow' }
    }
    stage('Deploy') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN')]) {
          // Compose cần file .env (env_file): dựng tạm từ .env.example + credential Vault, xoá ngay sau đó.
          sh '''
            { cat .env.example
              scripts/setup/vault_env.sh minio | sed 's/^export //'
              scripts/setup/vault_env.sh postgres | sed 's/^export //'; } > .env
            docker compose --profile airflow up -d --wait airflow
            bash scripts/setup/setup_airflow.sh
            rm -f .env
          '''
        }
      }
    }
    stage('DAG import') {
      steps {
        sh '''docker compose --profile airflow exec -T airflow python -c "
from airflow.models import DagBag
b = DagBag('/opt/airflow/dags', include_examples=False)
assert 'ml_rag_ingest' in b.dags and not b.import_errors, b.import_errors"'''
      }
    }
    stage('Run DAG') {
      steps {
        sh '''
          run=ci_$BUILD_NUMBER
          docker compose --profile airflow exec -T airflow airflow dags trigger ml_rag_ingest -r $run
          for i in $(seq 1 90); do
            state=$(docker compose --profile airflow exec -T airflow airflow dags list-runs -d ml_rag_ingest -o plain \
              | awk -v r="$run" '$2 == r {print $3}')
            echo "state=$state"; [ "$state" = success ] && exit 0; [ "$state" = failed ] && exit 1; sleep 10
          done; exit 1
        '''
      }
    }
  }
}
