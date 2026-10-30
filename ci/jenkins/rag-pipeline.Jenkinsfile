// CI/CD RAG Data Pipeline: test chunk/lineage → kiểm DAG import → build image Airflow
// (feast-venv có feast[postgres] + fastembed) → khởi động lại Airflow → chạy DAG ml_rag_ingest và chờ kết quả.
// Credential lấy từ Vault bằng token chỉ-đọc `vault-ci-token`.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds() }
  parameters {
    string(name: 'IMAGE_REGISTRY', defaultValue: 'localhost:5001', description: 'Registry namespace prefix; use ECR for cloud')
    choice(name: 'DATA_STORAGE', choices: ['minio', 's3'], description: 's3 = Jenkins trên EC2: dùng .env + overlay AWS của máy')
  }
  environment {
    DATA_STORAGE = "${params.DATA_STORAGE}"
    VAULT_ADDR = 'http://localhost:8200'
    REGISTRY = "${params.IMAGE_REGISTRY}"
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
          // Local: .env tạm từ .env.example + Vault, setup_airflow nạp Connection. AWS (s3): .env THẬT của EC2
          // (Ansible, /repo/.env) + overlay AWS; cấu hình Airflow nằm trong biến môi trường nên không setup_airflow.
          sh '''
            if [ "$DATA_STORAGE" = s3 ]; then
              install -m 600 /repo/.env .env
            else
              { cat .env.example
                scripts/setup/vault_env.sh minio | sed 's/^export //'
                scripts/setup/vault_env.sh postgres | sed 's/^export //'; } > .env
            fi
            scripts/run/ci_compose.sh --profile airflow up --no-build --pull always -d --wait airflow
            if [ "$DATA_STORAGE" != s3 ]; then bash scripts/setup/setup_airflow.sh; rm -f .env; fi
          '''
        }
      }
    }
    stage('DAG import') {
      steps {
        sh '''scripts/run/ci_compose.sh --profile airflow exec -T airflow python -c "
from airflow.models import DagBag
b = DagBag('/opt/airflow/dags', include_examples=False)
assert 'ml_rag_ingest' in b.dags and not b.import_errors, b.import_errors"'''
      }
    }
    stage('Run DAG') {
      steps {
        sh '''
          run=ci_$BUILD_NUMBER
          scripts/run/ci_compose.sh --profile airflow exec -T airflow airflow dags trigger ml_rag_ingest -r $run
          for i in $(seq 1 90); do
            state=$(scripts/run/ci_compose.sh --profile airflow exec -T airflow airflow dags list-runs -d ml_rag_ingest -o plain \
              | awk -v r="$run" '$2 == r {print $3}')
            echo "state=$state"; [ "$state" = success ] && exit 0; [ "$state" = failed ] && exit 1; sleep 10
          done; exit 1
        '''
      }
    }
  }
  post { always { sh 'rm -f .env' } }
}
