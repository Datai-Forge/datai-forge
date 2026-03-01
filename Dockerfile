# Stage 1: Extraction des binaires Node.js (Bookworm)
FROM node:20-bookworm-slim AS node_base

# Stage 2: Image Python principale (Bookworm)
FROM python:3.12-slim-bookworm

# Copie de Node.js pour Gemini CLI
COPY --from=node_base /usr/local/bin/node /usr/local/bin/
COPY --from=node_base /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

# Configuration
ENV PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# Dépendances système (JRE 17 + outils dev essentiels)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    git && \
    npm install -g @google/gemini-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Packages Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python3", "-m"]

# Par défaut, si aucun argument n'est passé, on lance le premier script bronze
CMD ["src.etl.bronze.bronze_presidentielle"]
