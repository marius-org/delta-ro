# DELTA-RO

> **Tactical Neon Protocol Enabled**

A top-down wave survival shooter with a neon cyberpunk aesthetic. Navigate a grid arena, eliminate enemies across escalating waves, and climb the leaderboard.

🎮 **Live:** [delta.slax.ro](https://delta.slax.ro)

---

## What It Does

The player controls a cyan unit inside a walled grid arena. Pink enemy units spawn each wave and chase the player. Survive and eliminate all enemies to advance to the next wave. The HUD tracks:

- **Score** — points accumulated from kills
- **Wave** — current wave number
- **Kills** — total enemies eliminated
- **Streak** — consecutive kills without taking damage
- **Vital Signs** — player health bar
- **Magazine Energy** — ammo/fire rate bar

---

## Tech Stack

| Layer | Technology |
|---|---|
| Game frontend | HTML5 Canvas + Vanilla JavaScript |
| Backend API | Python / FastAPI |
| Leaderboard DB | PostgreSQL |
| Containerization | Docker |
| Container registry | Docker Hub (`mariuseu/delta-ro`) |
| CI/CD | GitHub Actions (self-hosted runner on `control-node`) |
| GitOps | ArgoCD |
| Orchestration | Kubernetes (k3s HA cluster) |
| Ingress | ingress-nginx + Cloudflare Tunnel |
| Domain | [delta.slax.ro](https://delta.slax.ro) |

---

## Project Structure

```
delta-ro/
├── .github/
│   └── workflows/
│       └── deploy.yml       # CI/CD pipeline
├── backend/
│   ├── main.py              # FastAPI backend (score API)
│   └── requirements.txt     # Python dependencies
├── frontend/
│   └── index.html           # Game (HTML5 Canvas)
├── k3s/
│   ├── deployment.yaml      # Kubernetes Deployment + Service + Ingress
│   └── postgres.yaml        # PostgreSQL StatefulSet + PVC
└── Dockerfile
```

---

## CI/CD Pipeline

Every push to `main` triggers the GitHub Actions workflow:

1. **Build** Docker image
2. **Push** to Docker Hub with tag `mariuseu/delta-ro:<run_number>` and `:latest`
3. **Update** `k3s/deployment.yaml` with the new image tag
4. **Commit & push** the manifest change back to the repo
5. **ArgoCD** detects the change and auto-syncs the deployment to the k3s cluster

```
git push → GitHub Actions → Docker Hub → manifest update → ArgoCD → k3s cluster
```

---

## Running Locally

### Prerequisites

- Docker

### Steps

```bash
git clone https://github.com/marius-org/delta-ro.git
cd delta-ro
docker build -t delta-ro .
docker run -p 8000:8000 delta-ro
```

Open [http://localhost:8000](http://localhost:8000) in your browser.

---

## Kubernetes Deployment (k3s)

The app runs in its own namespace on a 3-master / 2-worker k3s HA cluster.

```bash
# Apply manually (normally handled by ArgoCD)
kubectl apply -f k3s/postgres.yaml
kubectl apply -f k3s/deployment.yaml
```

Traffic flow:

```
Internet → Cloudflare Tunnel → ingress-nginx (MetalLB) → delta-ro-service (ClusterIP) → pods
```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DB_HOST` | PostgreSQL host | `postgres` |
| `DB_NAME` | Database name | `delta` |
| `DB_USER` | Database user | `delta` |
| `DB_PASSWORD` | Database password | *(required)* |

---

## GitHub Secrets Required

| Secret | Description |
|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub username |
| `DOCKERHUB_TOKEN` | Docker Hub access token |
