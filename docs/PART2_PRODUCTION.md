# Part II: Production-Ready Improvements
## Orchestrator v5.0 - Production Roadmap

---

## 1. Hermes Agent Integration (nousresearch/hermes)

### Overview
Integrate real Hermes Agent from Nous Research as the primary executor agent.

### Architecture
```
┌─────────────────────────────────────────────────────────┐
│                    Orchestrator v5.0                     │
├─────────────────────────────────────────────────────────┤
│  Hermes Integration Layer                                │
│  ┌─────────────────────────────────────────────────┐   │
│  │ HermesAgentWrapper                                │   │
│  │ - Memory management                              │   │
│  │ - Skill loading                                  │   │
│  │ - Tool execution                                 │   │
│  │ - Session management                             │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│              Hermes Agent (nousresearch)                 │
│  - Terminal/TUI interface                              │
│  - 181+ built-in skills                                │
│  - Persistent memory                                   │
│  - MCP client                                         │
└─────────────────────────────────────────────────────────┘
```

### Installation
```bash
# Install Hermes Agent
npm install -g @hermes-agent/cli

# Configure
hermes config set model gpt-4
hermes config set memory.provider sqlite

# Start Hermes daemon
hermes daemon start
```

### Integration Code
```python
# hermes_agent_integration.py
class HermesAgentWrapper:
    def __init__(self, config: dict):
        self.hermes_path = config.get("hermes_path", "hermes")
        self.model = config.get("model", "gpt-4")
        self.memory_path = config.get("memory_path", "~/.hermes")

    def execute_task(self, task: str, context: dict = None) -> dict:
        """Execute task via Hermes Agent"""
        cmd = [
            self.hermes_path,
            "agent",
            "--prompt", task,
            "--model", self.model,
            "--memory", self.memory_path
        ]
        if context:
            for key, value in context.items():
                cmd.extend(["--context", f"{key}={value}"])

        result = subprocess.run(cmd, capture_output=True, text=True)
        return {
            "success": result.returncode == 0,
            "output": result.stdout,
            "error": result.stderr
        }

    def load_skill(self, skill_name: str) -> bool:
        """Load a skill into Hermes"""
        cmd = [self.hermes_path, "skill", "load", skill_name]
        return subprocess.run(cmd).returncode == 0
```

### Configuration
```yaml
# state/hermes_config.yaml
hermes:
  path: /usr/local/bin/hermes
  model: gpt-4
  memory_provider: sqlite
  memory_path: ~/.hermes/memories
  skills:
    - coding
    - research
    - analysis
  tools:
    - terminal
    - files
    - web
    - memory
  mcp:
    enabled: true
    servers:
      - github
      - filesystem
      - browser
```

---

## 2. MCP Server Bridge

### Overview
Create a bridge to connect MCP (Model Context Protocol) servers to the orchestrator.

### Architecture
```
┌─────────────────────────────────────────────────────────┐
│                   MCP Bridge Layer                        │
├─────────────────────────────────────────────────────────┤
│  MCPBridge                                               │
│  ┌───────────────┬───────────────┬───────────────┐     │
│  │ GitHub MCP   │ Filesystem    │ Browser MCP  │     │
│  │ Server       │ Server        │ Server       │     │
│  └───────┬───────┴───────┬───────┴───────┬───────┘     │
│          └───────────────┼───────────────┘              │
│                          ↓                               │
│              ┌───────────────────┐                       │
│              │ MCP Client Pool  │                       │
│              │ - Tool registry  │                       │
│              │ - Request routing│                       │
│              │ - Response cache │                       │
│              └───────────────────┘                       │
└─────────────────────────────────────────────────────────┘
```

### Implementation
```python
# mcp_bridge.py
class MCPBridge:
    """Bridge to MCP servers for tool standardization"""

    def __init__(self):
        self.servers = {}
        self.tools = {}

    def register_server(self, name: str, server_config: dict):
        """Register an MCP server"""
        self.servers[name] = {
            "config": server_config,
            "tools": self._discover_tools(name)
        }
        self.tools.update(self.servers[name]["tools"])

    def _discover_tools(self, server_name: str) -> dict:
        """Discover available tools from server"""
        # Tool discovery logic
        return {}

    def call_tool(self, tool_name: str, params: dict) -> dict:
        """Call a tool via MCP protocol"""
        for server_name, server in self.servers.items():
            if tool_name in server["tools"]:
                return self._send_request(server_name, tool_name, params)
        raise ValueError(f"Tool {tool_name} not found")

    def list_tools(self) -> List[dict]:
        """List all available tools"""
        return [
            {"name": name, **tool}
            for name, tool in self.tools.items()
        ]
```

### MCP Servers Configuration
```yaml
# state/mcp_config.yaml
mcp:
  servers:
    github:
      enabled: true
      auth_token_env: GITHUB_TOKEN
      tools:
        - create_issue
        - list_repos
        - create_pr

    filesystem:
      enabled: true
      allowed_paths:
        - /workspace
        - /tmp
      max_file_size: 100MB

    browser:
      enabled: true
      headless: true
      timeout: 30s
```

---

## 3. Kubernetes Deployment

### Overview
Production-ready Kubernetes manifests for cloud deployment.

### Manifests

#### deployment.yaml
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orchestrator
  labels:
    app: orchestrator
    version: v5.0
spec:
  replicas: 3
  selector:
    matchLabels:
      app: orchestrator
  template:
    metadata:
      labels:
        app: orchestrator
        version: v5.0
    spec:
      containers:
      - name: orchestrator
        image: orchestrator:5.0
        ports:
        - containerPort: 5000
        env:
        - name: OPENAI_API_KEY
          valueFrom:
            secretKeyRef:
              name: orchestrator-secrets
              key: openai-api-key
        - name: POSTGRES_HOST
          valueFrom:
            configMapKeyRef:
              name: orchestrator-config
              key: postgres-host
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 5000
          initialDelaySeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 5000
```

#### service.yaml
```yaml
apiVersion: v1
kind: Service
metadata:
  name: orchestrator-service
spec:
  selector:
    app: orchestrator
  ports:
  - name: http
    port: 80
    targetPort: 5000
  type: LoadBalancer
```

#### configmap.yaml
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: orchestrator-config
data:
  postgres-host: "postgres.default.svc.cluster.local"
  postgres-db: "orchestrator"
  redis-host: "redis.default.svc.cluster.local"
  rate-limit: "100"
  cache-ttl: "3600"
```

#### secrets.yaml
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orchestrator-secrets
type: Opaque
stringData:
  openai-api-key: "YOUR_API_KEY"
  postgres-password: "YOUR_DB_PASSWORD"
  jwt-secret: "YOUR_JWT_SECRET"
```

#### ingress.yaml
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: orchestrator-ingress
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt
spec:
  tls:
  - hosts:
    - orchestrator.example.com
    secretName: orchestrator-tls
  rules:
  - host: orchestrator.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: orchestrator-service
            port:
              number: 80
```

### Helm Chart Structure
```
orchestrator/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   ├── secrets.yaml
│   └── ingress.yaml
└── README.md
```

---

## 4. Monitoring (Prometheus + Grafana)

### Overview
Observability stack for production monitoring.

### Prometheus Metrics
```python
# metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Request metrics
request_count = Counter(
    'orchestrator_requests_total',
    'Total requests',
    ['endpoint', 'method', 'status']
)

request_duration = Histogram(
    'orchestrator_request_duration_seconds',
    'Request duration',
    ['endpoint']
)

# Task metrics
task_count = Counter(
    'orchestrator_tasks_total',
    'Total tasks',
    ['status', 'agent']
)

active_tasks = Gauge(
    'orchestrator_active_tasks',
    'Currently active tasks'
)

# Cache metrics
cache_hits = Counter(
    'orchestrator_cache_hits_total',
    'Cache hits'
)

cache_misses = Counter(
    'orchestrator_cache_misses_total',
    'Cache misses'
)
```

### Grafana Dashboard
```json
{
  "dashboard": {
    "title": "Orchestrator v5.0",
    "panels": [
      {
        "title": "Request Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(orchestrator_requests_total[5m])"
          }
        ]
      },
      {
        "title": "Task Completion",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(orchestrator_tasks_total{status='completed'}[5m])"
          }
        ]
      },
      {
        "title": "Cache Hit Rate",
        "type": "gauge",
        "targets": [
          {
            "expr": "orchestrator_cache_hits_total / (orchestrator_cache_hits_total + orchestrator_cache_misses_total) * 100"
          }
        ]
      }
    ]
  }
}
```

---

## 5. CI/CD Pipeline

### GitHub Actions Workflow
```yaml
# .github/workflows/orchestrator.yml
name: Orchestrator CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.12'

    - name: Install dependencies
      run: |
        pip install pytest pytest-cov
        pip install -r requirements.txt

    - name: Run tests
      run: |
        pytest --cov=orchestrator tests/

    - name: Lint
      run: |
        pip install pylint black
        black --check orchestrator/
        pylint orchestrator/

  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3

    - name: Build Docker image
      run: |
        docker build -t orchestrator:${{ github.sha }} .
        docker tag orchestrator:${{ github.sha }} orchestrator:latest

    - name: Push to registry
      if: github.ref == 'refs/heads/main'
      run: |
        echo ${{ secrets.DOCKER_TOKEN }} | docker login -u ${{ secrets.DOCKER_USER }} --password-stdin
        docker push orchestrator:${{ github.sha }}
        docker push orchestrator:latest

  deploy:
    needs: build
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
    - name: Deploy to K8s
      run: |
        kubectl set image deployment/orchestrator orchestrator=orchestrator:${{ github.sha }}
        kubectl rollout status deployment/orchestrator
```

### Dockerfile
```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create non-root user
RUN useradd -m orchestrator
USER orchestrator

EXPOSE 5000

CMD ["python", "orchestrator_v5.py"]
```

---

## Summary: Part II Implementation Status

| Improvement | Status | Notes |
|------------|--------|-------|
| Hermes Agent Integration | 🚀 Ready | Architecture + wrapper defined |
| MCP Server Bridge | 🚀 Ready | Architecture + bridge defined |
| Kubernetes Deployment | 🚀 Ready | Full manifests ready |
| Prometheus/Grafana | 🚀 Ready | Metrics + dashboard defined |
| CI/CD Pipeline | 🚀 Ready | GitHub Actions workflow ready |

---

## Next Steps

1. **Hermes Agent**: Install Hermes CLI, test integration
2. **MCP Bridge**: Deploy MCP servers, register tools
3. **K8s**: Apply manifests to cluster
4. **Monitoring**: Deploy Prometheus stack, import dashboard
5. **CI/CD**: Enable GitHub Actions, configure secrets
