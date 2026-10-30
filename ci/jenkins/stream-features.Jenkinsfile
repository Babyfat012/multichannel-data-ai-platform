// CI/CD cho stream feature job: test → build → deploy → kiểm tra.
// Job Flink `ml-sku-return-24h` + feast-pusher (Compose). Secret nằm ở Vault (secret/platform/*); Jenkins
// chỉ giữ token CHỈ-ĐỌC `vault-ci-token` (policy platform-read, scripts/setup/setup_vault.sh tạo).
// Agent cần Docker, docker compose, uv; chạy trên máy có stack (local hoặc VM GCE).
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds(); timeout(time: 30, unit: 'MINUTES') }
  parameters {
    string(name: 'IMAGE_REGISTRY', defaultValue: 'localhost:5001', description: 'Registry namespace prefix; use ECR for cloud')
    choice(name: 'DATA_STORAGE', choices: ['minio', 's3'], description: 's3 = Jenkins trên EC2: .env + overlay AWS/stream, Flink bản pipeline A')
  }
  environment {
    DATA_STORAGE = "${params.DATA_STORAGE}"
    CI_STREAM = '1'
    REGISTRY = "${params.IMAGE_REGISTRY}"
    TAG = "${env.GIT_COMMIT ?: env.BUILD_NUMBER}"
    POSTGRES_DB = 'sources'
    COMPOSE = 'scripts/run/ci_compose.sh --profile stream --profile flink --profile ml-stream'
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
              platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_sku_daily_features.py'''
      }
    }
    stage('Build') {
      steps {
        sh 'docker compose --profile stream --profile flink --profile ml-stream build flink-jobmanager feast-pusher'
      }
    }
    stage('Push') {
      steps { sh 'scripts/run/push_images.sh platform-flink:1.20.3-kafka3.4.0 platform-feast:0.66.0' }
    }
    stage('Release config') {
      steps {
        sh 'mkdir -p out'
        sh 'scripts/run/compose_release_images.sh flink-jobmanager=platform-flink flink-taskmanager=platform-flink feast-pusher=platform-feast > out/ci-compose-images.yaml'
      }
    }
    stage('Deploy') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN')]) {
          withEnv(['VAULT_ADDR=http://localhost:8200']) {
            // AWS: .env THẬT của EC2; registry Feast do DAG feast_apply (Airflow) quản ⇒ không apply từ CI; S3 qua IAM.
            sh '[ "$DATA_STORAGE" != s3 ] || install -m 600 /repo/.env .env'
            sh '$COMPOSE up --no-build --pull always -d --wait kafka flink-jobmanager flink-taskmanager'
            // Local: đăng ký view mới (nếu đổi features.py) trước khi pusher đọc registry.
            sh '[ "$DATA_STORAGE" = s3 ] || { set +x; eval "$(scripts/setup/vault_env.sh minio)" && $COMPOSE run --rm --no-deps feast-pusher feast apply; }'
            sh 'if [ "$DATA_STORAGE" != s3 ]; then set +x; eval "$(scripts/setup/vault_env.sh minio)"; fi; $COMPOSE up --no-build --pull always -d --force-recreate feast-pusher'
            // Huỷ job cùng tên rồi submit lại ⇒ không có hai job song song. AWS: bản A (Return phát lại, không tra PG).
            sh 'if [ "$DATA_STORAGE" = s3 ]; then export FEATURE_SQL=flink_jobs/ml/sku_return_feature_a.sql; fi; scripts/run/run_sku_return_feature.sh'
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
    always { sh 'rm -f .env' }
  }
}
