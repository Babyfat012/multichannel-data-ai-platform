// CI/CD Coordinator Agent (rubric mle dòng 35): kiểm manifest → prompt từ Langfuse → deploy → chờ Ready → agent card qua A2A gateway.
// API key gateway lấy từ Vault (secret/fsds/agentgateway) bằng token chỉ-đọc `vault-ci-token`.
pipeline {
  agent any
  options { timestamps(); disableConcurrentBuilds() }
  environment {
    KCTX = 'kind-fsds-ml'
    VAULT_ADDR = 'http://localhost:8200'
  }
  stages {
    stage('Validate') {
      steps {
        withCredentials([file(credentialsId: 'kind-kubeconfig', variable: 'KUBECONFIG')]) {
          sh 'kubectl --context $KCTX apply --dry-run=server -f infra/k8s/agents/coordinator.yaml'
        }
      }
    }
    stage('Sync prompts from Langfuse') {
      steps {
        withCredentials([file(credentialsId: 'kind-kubeconfig', variable: 'KUBECONFIG')]) {
          // System prompt lấy bản `production` trên Langfuse → ConfigMap agent-prompts (Agent đọc qua systemMessageFrom).
          sh 'python3 scripts/setup/sync_agent_prompts.py'
        }
      }
    }
    stage('Deploy') {
      steps {
        withCredentials([file(credentialsId: 'kind-kubeconfig', variable: 'KUBECONFIG')]) {
          sh '''
            kubectl --context $KCTX apply -f infra/k8s/agents/coordinator.yaml
            kubectl --context $KCTX -n kagent wait agent/coordinator --for=condition=Ready --timeout=180s
          '''
        }
      }
    }
    stage('Smoke A2A') {
      steps {
        withCredentials([string(credentialsId: 'vault-ci-token', variable: 'VAULT_TOKEN'),
                         file(credentialsId: 'kind-kubeconfig', variable: 'KUBECONFIG')]) {
          sh '''
            eval "$(scripts/setup/vault_env.sh agentgateway)"
            kubectl --context $KCTX -n agentgateway run smoke-$BUILD_NUMBER --rm -i --restart=Never \
              --image=curlimages/curl:8.11.1 --env=KEY="$AGW_AGENT_KEY" -- sh -c \
              'curl -sf -H "Authorization: Bearer $KEY" http://agentgateway:3000/a2a/coordinator/.well-known/agent-card.json' \
              | grep -q '"name":"coordinator"'
          '''
        }
      }
    }
  }
}
