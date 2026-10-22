// CI/CD cho stream feature job (rubric mle dòng 36): test → build → deploy → kiểm tra.
// Job Flink `ml-sku-return-24h` + feast-pusher (Compose). Secret nằm ở Vault (secret/fsds/*); Jenkins
// chỉ giữ token CHỈ-ĐỌC `vault-ci-token` (policy fsds-read, scripts/setup/setup_vault.sh tạo).
// Agent cần Docker, docker compose, uv; chạy trên máy có stack (local hoặc VM GCE).
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 30, unit: 'MINUTES') }
  environment {
    POSTGRES_DB = 'sources'
    COMPOSE = 'docker compose --profile stream --profile flink --profile ml-stream'
  }
  stages {
    stage('Test') {
      steps {
        sh 'uv sync --frozen'
        sh 'uv run ruff format --check feature_repo ml && uv run ruff check feature_repo ml'
        // Logic Feast + định nghĩa view; test Spark của feature theo ngày chạy trong image Airflow.
        sh 'uv run pytest -q tests/unit/test_docstrings.py tests/unit/generator/test_publish_order_lifecycle_events.py'
        sh '''docker run --rm --network none --read-only --tmpfs /tmp:rw,exec,size=1g \
              --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
              fsds-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_sku_daily_features.py'''
      }
    }
    stage('Build') {
      steps {
        sh '$COMPOSE build flink-jobmanager feast-pusher'
      }
    }
    stage('Deploy') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN')]) {
          withEnv(['VAULT_ADDR=http://localhost:8200']) {
            sh '$COMPOSE up -d --wait kafka flink-jobmanager flink-taskmanager'
            // Đăng ký view mới (nếu đổi features.py) trước khi pusher đọc registry.
            sh 'eval "$(scripts/setup/vault_env.sh minio)" && $COMPOSE run --rm --no-deps feast-pusher feast apply'
            sh 'eval "$(scripts/setup/vault_env.sh minio)" && $COMPOSE up -d --force-recreate feast-pusher'
            // Huỷ job cùng tên rồi submit lại ⇒ không có hai job song song; credential PG lấy từ Vault.
            sh 'scripts/run/run_sku_return_feature.sh'
          }
        }
      }
    }
    stage('Verify') {
      steps {
        sh '''for i in $(seq 1 30); do
                curl -fsS localhost:${FLINK_UI_PORT:-8082}/jobs/overview | python3 -c "import json,sys
jobs=[j for j in json.load(sys.stdin)['jobs'] if j['name']=='ml-sku-return-24h' and j['state']=='RUNNING']
sys.exit(0 if len(jobs)==1 else 1)" && exit 0; sleep 5; done; exit 1'''
        sh '$COMPOSE ps feast-pusher --format "{{.State}}" | grep -qx running'
      }
    }
  }
  post {
    failure { sh '$COMPOSE logs --tail 50 feast-pusher flink-jobmanager || true' }
  }
}
