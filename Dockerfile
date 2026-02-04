FROM node:20-slim

WORKDIR /app

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update && apt-get install -y \
  ca-certificates \
  wget \
  --no-install-recommends && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install

RUN npx playwright install --with-deps chromium

COPY . .

ENV PORT=10000
EXPOSE 10000

CMD ["node", "server.js"]
