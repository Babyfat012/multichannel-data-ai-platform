<!-- Mục đích: Jenkins CI/CD local cho phần MLE (rubric mle dòng 33–36): dựng, cấu hình, job, secret. -->
# CI/CD — Jenkins

Rubric `mle` dòng 33–36: CI/CD (test + build + auto-deploy) cho RAG pipeline, MCP server + agent,
coordinator, stream feature job. Secret phải nằm ở Jenkins/công cụ tương tự, không trong code.

## Dựng

| Thành phần | File |
|---|---|
| Image Jenkins 2.580.1 LTS + plugin cố định + docker/compose/kubectl/helm/kind/uv | [`docker/Dockerfile.jenkins`](../docker/Dockerfile.jenkins) |
| Cấu hình bằng code (JCasC): user, credential, 4 job | [`infra/jenkins/jenkins.yaml`](../infra/jenkins/jenkins.yaml) |
| Khởi động (lấy secret từ Vault) | [`scripts/setup/setup_jenkins.sh`](../scripts/setup/setup_jenkins.sh), service `jenkins` (profile `ci`) |
| Pipeline | [`ci/jenkins/`](../ci/jenkins/stream-features.Jenkinsfile): `stream-features` (dòng 36), `feature-api-agents` (34), `coordinator` (35), `rag-pipeline` (33) |

```bash
scripts/setup/setup_vault.sh      # trước: Vault + token chỉ-đọc
scripts/setup/setup_jenkins.sh    # UI http://localhost:8090, user admin, mật khẩu ở Vault secret/fsds/jenkins
```

## Quyết định

- **Secret**: Jenkins chỉ giữ 2 credential — `vault-ci-token` (token Vault chỉ-đọc) và
  `kind-kubeconfig`. Mật khẩu DB/MinIO lấy từ Vault lúc build (`scripts/setup/vault_env.sh`).
  Mật khẩu admin Jenkins sinh ngẫu nhiên, cất ở Vault, nạp qua biến môi trường mỗi lần khởi động.
- **Job lấy code từ git `main`** (`file:///repo`): CI chạy đúng code đã commit, không phải working
  tree. ⇒ Phải commit các file `ci/jenkins/*` trước khi build lần đầu.
- **network_mode host + JENKINS_HOME cùng đường dẫn với host**: build gọi `docker compose` qua socket
  của host; bind mount tương đối trong workspace phải tồn tại trên host ở đúng đường dẫn đó.
- Build chạy trên controller (1 executor) — đủ cho local; trên GKE tách agent.

## Bằng chứng (2026-10-21)

- Jenkins `Up`, `/login` 200; API liệt kê job `coordinator`, `feature-api-agents`, `rag-pipeline`,
  `stream-features` và credential `vault-ci-token`, `kind-kubeconfig` — đều do JCasC tạo.
- Đổi mật khẩu admin: xoá `secret/fsds/jenkins` rồi chạy lại `setup_jenkins.sh` (đã làm một lần, login
  bằng mật khẩu mới OK).
- **Chưa build**: các Jenkinsfile chưa được commit lên `main`.

<!-- Ảnh Jenkins: danh sách job + Stage View từng pipeline sau lần build đầu: người học chụp. -->
