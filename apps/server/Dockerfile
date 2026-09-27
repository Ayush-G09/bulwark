FROM node:22-alpine
WORKDIR /app

COPY package.json package-lock.json tsconfig.base.json ./
COPY packages/core/package.json packages/core/
COPY packages/distributed/package.json packages/distributed/
COPY apps/server/package.json apps/server/
RUN npm ci

COPY packages/core packages/core
COPY packages/distributed packages/distributed
COPY apps/server apps/server

ENV NODE_ENV=production
EXPOSE 3300
CMD ["npx", "tsx", "apps/server/src/main.ts"]
